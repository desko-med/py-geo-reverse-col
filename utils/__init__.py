import glob
import datetime
import random
import string

from dateutil import tz


def list_files_in_dir(path, ext):
    """
    List all files in a directory
    :return tuple
    """

    files = sorted(glob.glob(path + f'*{ext}'), key=str.lower)
    return tuple(files)


def create_unique_file_name(event_name: str, extension: str = "csv", timestamp=None):
    """
    Create an S3 key name based on the specified timestamp
    Params
    :param timestamp:
    :param extension: str -> default csv
    :param event_name: str -> String
    """

    # generate a random string to avoid key name conflicts from @mattgemmell at
    # http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
    if not timestamp:
        timestamp = datetime.datetime.now()

    nonce = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(8))

    # make sure we have a unique key name, ideally full timestamp + nonce
    # key_name = '{}-{}-{}.{}'.format(event_name, str(timestamp), nonce, extension)

    # if type(datetime.datetime.now()) == type(timestamp):
    # partition = f"year=%Y/month=%m/{event_name}_%Y-%m-%d-%H-%M-%S-%f"
    partition = f"year=%Y/month=%m/day=%d/{event_name}_%Y-%m-%d"
    key_name = '{}-{}.{}'.format(timestamp.strftime(partition), nonce, extension)

    return key_name


def get_local_time(timestamp):
    """
    Funcion para devolver la fecha local en formato RFC3339
    https://www.rfc-editor.org/rfc/rfc3339
    :param timestamp:
    :return: datetime
    """
    col = tz.gettz('America/Bogota')
    # Se pasa la fecha a formato RFC3339
    dt = datetime.datetime.fromtimestamp(timestamp)
    loc_dt = tz.resolve_imaginary(dt.replace(microsecond=0).astimezone(col))

    return loc_dt.isoformat('T')
