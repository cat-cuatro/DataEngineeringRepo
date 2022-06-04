import os
import time
from main import consumerScriptCall

def main():
    while True:
        print('CTRL-C to stop at any time.')
        consumerScriptCall()
        print('Consumer called. Waiting 1.5 minutes.')
        time.sleep(90)

if __name__ == "__main__":
    main()
