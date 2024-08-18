import logging

class Logger:
    def __init__(self, app_name):
        logging.basicConfig(
            level=logging.INFO,
            format=f'[{app_name}][%(levelname)s][%(asctime)s.%(msecs)03d]%(message)s',
            datefmt='%Y-%m%d %H:%M:%S'
        )
        self.logger = logging.getLogger(app_name)
        self.app_name = app_name
        self.step_num = 0
        self.last_epoch_id = 0

    def write_log(self, log_type: str, msg: str, epoch_id: int):
        log_type_lst = ['debug','info','warning','error','critical']
        if log_type.lower() not in log_type_lst: return

        if self.last_epoch_id == epoch_id:
            self.step_num += 1
        else:
            self.step_num = 1
            self.last_epoch_id = epoch_id

        if epoch_id is None:
            getattr(self.logger, log_type.lower())(f'[STEP{self.step_num}] {msg}')
        else:
            getattr(self.logger, log_type.lower())(f'[EPOCH:{epoch_id}][STEP{self.step_num}] {msg}')