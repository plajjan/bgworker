# -*- mode: python; python-indent: 4 -*-
import logging
import random
import sys
import time

import ncs
from ncs.application import Service

from . import background_process

def bg_worker():
    log = logging.getLogger()

    while True:
        with ncs.maapi.single_write_trans('bgworker', 'system', db=ncs.OPERATIONAL) as oper_trans_write:
            root = ncs.maagic.get_root(oper_trans_write)
            cur_val = root.bgworker.counter
            root.bgworker.counter += 1
            oper_trans_write.apply()

        log.debug("Hello from background worker process, increment counter from {} to {}".format(cur_val, cur_val+1))
        log.info("Hello from background worker process, increment counter from {} to {}".format(cur_val, cur_val+1))
        log.warning("Hello from background worker process, increment counter from {} to {}".format(cur_val, cur_val+1))
        log.error("Hello from background worker process, increment counter from {} to {}".format(cur_val, cur_val+1))
        log.critical("Hello from background worker process, increment counter from {} to {}".format(cur_val, cur_val+1))
#        if random.randint(0, 10) == 9:
#            log.error("Bad dice value")
#            sys.exit(1)
        time.sleep(2)

class Main(ncs.application.Application):
    def setup(self):
        self.log.info('Main RUNNING')
        self.p = background_process.Process(self, bg_worker, config_path='/bgworker/enabled')
        self.p.start()

    def teardown(self):
        self.log.info('Main FINISHED')
        self.p.stop()
