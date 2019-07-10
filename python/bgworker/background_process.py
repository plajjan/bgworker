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
import typing

import ncs
from ncs.experimental import Subscriber
# queue module is called Queue in py2, we import with py3 name since the
# exposed interface is similar enough
try:
    import queue
except ImportError:
    import Queue as queue


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


def _bg_wrapper(bg_fun, q, log_level, *bg_fun_args):
    """Internal wrapper for the background worker function.

    Used to set up logging via a QueueHandler in the child process. The other end
    of the queue is observed by a QueueListener in the parent process.
    """
    queue_hdlr = logging.handlers.QueueHandler(q)
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(queue_hdlr)
    logger = logging.getLogger(bg_fun.__name__)
    bg_fun(logger, *bg_fun_args)


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

        self.log = app.log
        self.name = "{}.{}".format(self.app.__class__.__module__,
                                   self.app.__class__.__name__)

        self.mp_ctx = multiprocessing.get_context('spawn')
        self.log.info("{} supervisor starting".format(self.name))
        self.q = queue.Queue()

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
        self.log.debug("Found handlers: ", hdlrs)
        self.log_queue = self.mp_ctx.Queue()
        self.queue_listener = logging.handlers.QueueListener(self.log_queue, *hdlrs, respect_handler_level=True)
        self.queue_listener.start()
        self.current_log_level = self.app._logger.getEffectiveLevel()

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
                    # check if HA is enabled
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
            should_run = self.config_enabled and (not self.ha_enabled or self.ha_master)

            if should_run and (self.worker is None or not self.worker.is_alive()):
                self.log.info("Background worker process should run but is not running, starting")
                if self.worker is not None:
                    self.worker_stop()
                self.worker_start()
            if self.worker is not None and self.worker.is_alive() and not should_run:
                self.log.info("Background worker process is running but should not run, stopping")
                self.worker_stop()

            try:
                item = self.q.get(timeout=1)
            except queue.Empty:
                continue

            k, v = item
            if k == 'exit':
                return
            elif k == 'enabled':
                self.config_enabled = v


    def stop(self):
        """stop is called when the supervisor thread should stop and is part of
        the standard Python interface for threading.Thread
        """
        # stop the HA event listener
        self.ha_event_listener.stop()

        # stop CDB subscriber
        if self.config_path is not None:
            self.config_subscriber.stop()

        # stop the logging QueueListener
        self.queue_listener.stop()

        # stop us, the supervisor
        self.q.put(('exit', None))
        self.join()
        self.app.del_running_thread(self.name + ' (Supervisor)')

        # stop the background worker process
        self.worker_stop()


    def worker_start(self):
        """Starts the background worker process
        """
        self.log.info("{}: starting the background worker process".format(self.name))
        # Instead of using the usual worker thread, we use a separate process here.
        # This allows us to terminate the process on package reload / NSO shutdown.

        # Instead of calling the bg_fun worker function directly, call our
        # internal wrapper to set up inter-process logging through a queue.
        args = [self.bg_fun, self.log_queue, self.current_log_level] + self.bg_fun_args
        self.worker = self.mp_ctx.Process(target=_bg_wrapper, args=args)
        self.worker.start()


    def worker_stop(self):
        """Stops the background worker process
        """
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
        events.notifications_connect(event_socket, mask, ip='127.0.0.1', port=ncs.NCS_PORT)
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
                self.q.put(('ha-mode', 'master'))
            elif ha_notif_type == events.HA_INFO_IS_NONE:
                self.q.put(('ha-mode', 'none'))

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
