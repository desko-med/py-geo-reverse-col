import logging

from settings import Settings


class AppLogger:

    @staticmethod
    def print_log(log_type: str, step: str, msg: str):
        """
        Method to print log in console.
        :param log_type: str -> use: info | error | warning
        :param step: str -> use: self.__class__.__name__
        :param msg: str -> user: any
        :return: looger.log
        """
        log_types = {
            'info': 20,
            'error': 40,
            'warning': 30
        }
        logger = logging.getLogger(Settings.APP_NAME)
        return logger.log(log_types.get(log_type), f"{Settings.APP_NAME} - {step} - {msg}")


class AppDataObs(AppLogger):
    pass


DataObs = AppDataObs()
