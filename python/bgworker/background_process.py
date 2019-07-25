# -*- mode: python; python-indent: 4 -*-
"""A micro-framework for running background processes in Cisco NSO Python VM.

Running any kind of background workers in Cisco NSO can be rather tricky. This
will help you out! Just define a function that does what you want and create a
Process instance to run it!

We react to:
 - background worker process dying (will restart it)
 - NCS package events, like redeploy
 - configuration changes (disable the background worker)
 - HA events (if we are a slave)
"""
import logging
import logging.handlers
import multiprocessing
import os
import select
import socket
import threading
import time
import traceback
import typing

import ncs
from ncs.experimental import Subscriber


def _get_handler_impls(logger: logging.Logger) -> typing.Iterable[logging.Handler]:
    """For a given Logger instance, find the registered handlers.

    A Logger instance may have handlers registered in the 'handlers' list.
    Usually there is one handler registered to the Root Logger.
    This function uses the same algorithm as Logger.callHandlers to find
    all relevant handlers.
    """

    c = logger
    while c:
        for hdlr in c.handlers:
            yield hdlr
        if not c.propagate:
            c = None    #break out
        else:
            c = c.parent


def _bg_wrapper(pipe_unused, log_q, log_config_q, log_level, bg_fun, *bg_fun_args):
    """Internal wrapper for the background worker function.

    Used to set up logging via a QueueHandler in the child process. The other end
    of the queue is observed by a QueueListener in the parent process.
    """
    queue_hdlr = logging.handlers.QueueHandler(log_q)
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(queue_hdlr)

    # thread to monitor log level changes and reconfigure the root logger level
    log_reconf = LogReconfigurator(log_config_q, root)
    log_reconf.start()

    try:
        bg_fun(*bg_fun_args)
    except Exception as e:
        root.error('Unhandled error in {} - {}: {}'.format(bg_fun.__name__, type(e).__name__, e))
        root.debug(traceback.format_exc())


class LogReconfigurator(threading.Thread):
    def __init__(self, q, log_root):
        super(LogReconfigurator, self).__init__()
        self.daemon = True
        self.q = q
        self.log_root = log_root

    def run(self):
        while True:
            k, v = self.q.get()
            if k is 'exit':
                return

            self.log_root.setLevel(v)

    def stop(self):
        self.q.put(('exit', None))


class Process(threading.Thread):
    """Supervisor for running the main background process and reacting to
    various events
    """
    def __init__(self, app, bg_fun, bg_fun_args=None, config_path=None):
        super(Process, self).__init__()
        self.app = app
        self.bg_fun = bg_fun
        if bg_fun_args is None:
            bg_fun_args = []
        self.bg_fun_args = bg_fun_args
        self.config_path = config_path
        self.parent_pipe = None

        self.log = app.log
        self.name = "{}.{}".format(self.app.__class__.__module__,
                                   self.app.__class__.__name__)
        self.log.info("{} supervisor starting".format(self.name))

        self.vmid = self.app._ncs_id

        self.mp_ctx = multiprocessing.get_context('spawn')
        self.q = self.mp_ctx.Queue()

        # start the config subscriber thread
        if self.config_path is not None:
            self.config_subscriber = Subscriber(app=self.app, log=self.log)
            subscriber_iter = ConfigSubscriber(self.q, self.config_path)
            subscriber_iter.register(self.config_subscriber)
            self.config_subscriber.start()

        # start the HA event listener thread
        self.ha_event_listener = HaEventListener(app=self.app, q=self.q)
        self.ha_event_listener.start()

        # start the logging QueueListener thread
        hdlrs = list(_get_handler_impls(self.app._logger))
        self.log_queue = self.mp_ctx.Queue()
        self.queue_listener = logging.handlers.QueueListener(self.log_queue, *hdlrs, respect_handler_level=True)
        self.queue_listener.start()
        self.current_log_level = self.app._logger.getEffectiveLevel()

        # start log config CDB subscriber
        self.log_config_q = self.mp_ctx.Queue()
        self.log_config_subscriber = Subscriber(app=self.app, log=self.log)
        log_subscriber_iter = LogConfigSubscriber(self.log_config_q, self.vmid)
        log_subscriber_iter.register(self.log_config_subscriber)
        self.log_config_subscriber.start()

        self.worker = None

        # Read initial configuration, using two separate transactions
        with ncs.maapi.Maapi() as m:
            with ncs.maapi.Session(m, '{}_supervisor'.format(self.name), 'system'):
                # in the 1st transaction read config data from the 'enabled' leaf
                with m.start_read_trans() as t_read:
                    if config_path is not None:
                        enabled = t_read.get_elem(self.config_path)
                        self.config_enabled = bool(enabled)
                    else:
                        # if there is no config_path we assume the process is always enabled
                        self.config_enabled = True

                # In the 2nd transaction read operational data regarding HA.
                # This is an expensive operation invoking a data provider, thus
                # we don't want to incur any unnecessary locks
                with m.start_read_trans(db=ncs.OPERATIONAL) as oper_t_read:
                    # Check if HA is enabled. This can only be configured in
                    # ncs.conf and requires NSO restart, so it is fine if we
                    # just read it the once.
                    if oper_t_read.exists("/tfnm:ncs-state/tfnm:ha"):
                        self.ha_enabled = True
                    else:
                        self.ha_enabled = False

                    # determine HA state if HA is enabled
                    if self.ha_enabled:
                        ha_mode = str(ncs.maagic.get_node(oper_t_read, '/tfnm:ncs-state/tfnm:ha/tfnm:mode'))
                        self.ha_master = (ha_mode == 'master')


    def run(self):
        self.app.add_running_thread(self.name + ' (Supervisor)')

        while True:
            try:
                should_run = self.config_enabled and (not self.ha_enabled or self.ha_master)

                if should_run and (self.worker is None or not self.worker.is_alive()):
                    self.log.info("Background worker process should run but is not running, starting")
                    if self.worker is not None:
                        self.worker_stop()
                    self.worker_start()
                if self.worker is not None and self.worker.is_alive() and not should_run:
                    self.log.info("Background worker process is running but should not run, stopping")
                    self.worker_stop()

                # check for input
                waitable_rfds = [self.q._reader]
                if should_run:
                    waitable_rfds.append(self.parent_pipe)

                rfds, _, _ = select.select(waitable_rfds, [], [])
                for rfd in rfds:
                    if rfd == self.q._reader:
                        k, v = self.q.get()

                        if k == 'exit':
                            return
                        elif k == 'enabled':
                            self.config_enabled = v
                        elif k == "ha-master":
                            self.ha_master = v

                    if rfd == self.parent_pipe:
                        # getting a readable event on the pipe should mean the
                        # child is dead - wait for it to die and start again
                        # we'll restart it at the top of the loop
                        self.log.info("Child process died")
                        if self.worker.is_alive():
                            self.worker.join()

            except Exception as e:
                self.log.error('Unhandled exception in the supervisor thread: {} ({})'.format(type(e).__name__, e))
                self.log.debug(traceback.format_exc())
                time.sleep(1)


    def stop(self):
        """stop is called when the supervisor thread should stop and is part of
        the standard Python interface for threading.Thread
        """
        # stop the HA event listener
        self.log.debug("{}: stopping HA event listener".format(self.name))
        self.ha_event_listener.stop()

        # stop config CDB subscriber
        self.log.debug("{}: stopping config CDB subscriber".format(self.name))
        if self.config_path is not None:
            self.config_subscriber.stop()

        # stop log config CDB subscriber
        self.log.debug("{}: stopping log config CDB subscriber".format(self.name))
        self.log_config_subscriber.stop()

        # stop the logging QueueListener
        self.log.debug("{}: stopping logging QueueListener".format(self.name))
        self.queue_listener.stop()

        # stop us, the supervisor
        self.log.debug("{}: stopping supervisor thread".format(self.name))

        self.q.put(('exit', None))
        self.join()
        self.app.del_running_thread(self.name + ' (Supervisor)')

        # stop the background worker process
        self.log.debug("{}: stopping background worker process".format(self.name))
        self.worker_stop()


    def worker_start(self):
        """Starts the background worker process
        """
        self.log.info("{}: starting the background worker process".format(self.name))
        # Instead of using the usual worker thread, we use a separate process here.
        # This allows us to terminate the process on package reload / NSO shutdown.

        # using multiprocessing.Pipe which is shareable across a spawned
        # process, while os.pipe only works, per default over to a forked
        # child
        self.parent_pipe, child_pipe = self.mp_ctx.Pipe()

        # Instead of calling the bg_fun worker function directly, call our
        # internal wrapper to set up things like inter-process logging through
        # a queue.
        args = [child_pipe, self.log_queue, self.log_config_q, self.current_log_level, self.bg_fun] + self.bg_fun_args
        self.worker = self.mp_ctx.Process(target=_bg_wrapper, args=args)
        self.worker.start()

        # close child pipe in parent so only child is in possession of file
        # handle, which means we get EOF when the child dies
        child_pipe.close()


    def worker_stop(self):
        """Stops the background worker process
        """
        if self.worker is None:
            self.log.info("{}: asked to stop worker but background worker does not exist".format(self.name))
            return
        if self.worker.is_alive():
            self.log.info("{}: stopping the background worker process".format(self.name))
            self.worker.terminate()
        self.worker.join(timeout=1)
        if self.worker.is_alive():
            self.log.error("{}: worker not terminated on time, alive: {}  process: {}".format(self, self.worker.is_alive(), self.worker))



class ConfigSubscriber(object):
    """CDB subscriber for background worker process

    It is assumed that there is an 'enabled' leaf that controls whether a
    background worker process should be enabled or disabled. Given the path to
    that leaf, this subscriber can monitor it and send any changes to the
    supervisor which in turn starts or stops the background worker process.

    The enabled leaf has to be a boolean where true means the background worker
    process is enabled and should run.
    """
    def __init__(self, q, config_path):
        self.q = q
        self.config_path = config_path

    def register(self, subscriber):
        subscriber.register(self.config_path, priority=101, iter_obj=self)

    def pre_iterate(self):
        return {'enabled': False}

    def iterate(self, keypath_unused, operation_unused, oldval_unused, newval, state):
        state['enabled'] = newval
        return ncs.ITER_RECURSE

    def should_post_iterate(self, state_unused):
        return True

    def post_iterate(self, state):
        self.q.put(("enabled", bool(state['enabled'])))


class LogConfigSubscriber(object):
    """CDB subscriber for python-vm logging level

    This subscribers monitors /python-vm/logging/level and
    /python-vm/logging/vm-levels{vmid}/level and propagates any changes to the
    child process which can then in turn log at the appropriate level. VM
    specific level naturally take precedence over the global level.
    """
    def __init__(self, q, vmid):
        self.q = q
        self.vmid = vmid

        # read in initial values for these nodes
        # a CDB subscriber only naturally gets changes but if we are to emit a
        # sane value we have to know what both underlying (global & vm specific
        # log level) are and thus must read it in first
        with ncs.maapi.single_read_trans('', 'system') as t_read:
            try:
                self.global_level = ncs.maagic.get_node(t_read, '/python-vm/logging/level')
            except Exception:
                self.global_level = None
            try:
                self.vm_level = ncs.maagic.get_node(t_read, '/python-vm/logging/vm-levels{{{}}}/level'.format(self.vmid))
            except Exception:
                self.vm_level = None

    def register(self, subscriber):
        subscriber.register('/python-vm/logging/level', priority=101, iter_obj=self)
        # we don't include /level here at the end as then we won't get notified
        # when the whole vm-levels list entry is deleted, we still get when
        # /levels is being set though (and it is mandatory so we know it will
        # always be there for whenever a vm-levels list entry exists)
        subscriber.register('/python-vm/logging/vm-levels{{{}}}'.format(self.vmid), priority=101, iter_obj=self)

    def pre_iterate(self):
        return {}

    def iterate(self, keypath, operation_unused, oldval_unused, newval, unused_state):
        if str(keypath[-3]) == "vm-levels":
            self.vm_level = newval
        else:
            self.global_level = newval
        return ncs.ITER_RECURSE

    def should_post_iterate(self, unused_state):
        return True

    def post_iterate(self, unused_state):
        # the log level enum in the YANG model maps the integer values to
        # python log levels quite nicely just by adding 1 and multiplying by 10
        configured_level = int(self.vm_level or self.global_level)
        new_level = (configured_level+1)*10
        self.q.put(("log-level", new_level))


class HaEventListener(threading.Thread):
    """HA Event Listener
    HA events, like HA-mode transitions, are exposed over a notification API.
    We listen on that and forward relevant messages over the queue to the
    supervisor which can act accordingly.

    We use a WaitableEvent rather than a threading.Event since the former
    allows us to wait on it using a select loop. The HA events are received
    over a socket which can also be waited upon using a select loop, thus
    making it possible to wait for the two inputs we have using a single select
    loop.
    """
    def __init__(self, app, q):
        super(HaEventListener, self).__init__()
        self.app = app
        self.log = app.log
        self.q = q
        self.log.info('{} supervisor: init'.format(self))
        self.exit_flag = WaitableEvent()

    def run(self):
        self.app.add_running_thread(self.__class__.__name__ + ' (HA event listener)')

        self.log.info('run() HA event listener')
        from _ncs import events
        mask = events.NOTIF_HA_INFO
        event_socket = socket.socket()
        events.notifications_connect(event_socket, mask, ip='127.0.0.1', port=ncs.PORT)
        while True:
            rl, _, _ = select.select([self.exit_flag, event_socket], [], [])
            if self.exit_flag in rl:
                event_socket.close()
                return

            notification = events.read_notification(event_socket)
            # Can this fail? Could we get a KeyError here? Afraid to catch it
            # because I don't know what it could mean.
            ha_notif_type = notification['hnot']['type']

            if ha_notif_type == events.HA_INFO_IS_MASTER:
                self.q.put(('ha-master', True))
            elif ha_notif_type == events.HA_INFO_IS_NONE:
                self.q.put(('ha-master', False))
            elif ha_notif_type == events.HA_INFO_SLAVE_INITIALIZED:
                self.q.put(('ha-master', False))

    def stop(self):
        self.exit_flag.set()
        self.join()
        self.app.del_running_thread(self.__class__.__name__ + ' (HA event listener)')


class WaitableEvent:
    """Provides an abstract object that can be used to resume select loops with
    indefinite waits from another thread or process. This mimics the standard
    threading.Event interface."""
    def __init__(self):
        self._read_fd, self._write_fd = os.pipe()

    def wait(self, timeout=None):
        rfds, _, _ = select.select([self._read_fd], [], [], timeout)
        return self._read_fd in rfds

    def is_set(self):
        return self.wait(0)

    def isSet(self):
        return self.wait(0)

    def clear(self):
        if self.isSet():
            os.read(self._read_fd, 1)

    def set(self):
        if not self.isSet():
            os.write(self._write_fd, b'1')

    def fileno(self):
        """Return the FD number of the read side of the pipe, allows this
        object to be used with select.select()
        """
        return self._read_fd

    def __del__(self):
        os.close(self._read_fd)
        os.close(self._write_fd)
