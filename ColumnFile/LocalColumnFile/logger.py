
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Logger:

    def __init__(self, active):
        self.active = active
    
    def log(self, msg):
        if not self.active: return;
        print('(column-file)> ', end = '')
        if msg.startswith('[ERROR]', 0, 7): color = bcolors.FAIL
        elif msg.startswith('[OK]', 0, 4): color = bcolors.OKGREEN
        elif msg.startswith('[WARN]', 0, 6): color = bcolors.WARNING
        else: color = ''
        print(color + msg + bcolors.ENDC)
