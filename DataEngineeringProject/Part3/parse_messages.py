import re
import database_ops
import time

class Parser():
    def __init__(self):
        self.data = []
        self.pattern_keys = {
                "BREADCRUMBS": r'(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+-\w+-\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(|\d+)[\"|\s|\,|\w]+: "(|\d+)[\"|\s|\,|\w]+: "()[\"|\s|\,|\w]+: "(-\d+.\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(-\d+.\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(\d+|)[\"|\s|\,|\w]+: "(\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(|-\d+|\d+)"'
        }
        self.keys = [
                'EVENT_NO_TRIP', 
                'EVENT_NO_STOP', 
                'OPD_DATE',
                'VEHICLE_ID',
                'METERS',
                'ACT_TIME',
                'VELOCITY',
                'DIRECTION',
                'RADIO_QUALITY',
                'GPS_LONGITUDE',
                'GPS_LATITUDE',
                'GPS_SATELLITES',
                'GPS_HDOP',
                'SCHEDULE_DEVIATION',
        ]
        self.structured_data = self.initialize_structure()
    def initialize_structure(self):
        to_return = {}
        for key in self.keys:
            to_return.update({key : []})
        return to_return

    def parse(self, string):
        #time.sleep(0.1)
        matches = re.findall(self.pattern_keys["BREADCRUMBS"], string)
        # After using the world's ugliest regex string I presumably have all of the breadcrumb data in-order.
        try:
            self.sort_match_data(matches[0])
        except IndexError:
            pass
            print('NO MATCHES!!')
            print(string)
        #print('now storing into the database..')
        return matches

    def sort_match_data(self, matches):
        for i in range(len(self.keys)):
            temp = self.structured_data.get(self.keys[i])
            try:
                temp.append(matches[i])
            except IndexError:
                temp.append("")
            self.structured_data.update({self.keys[i] : temp})
        #print(self.structured_data)
    
    def parse_data_fields(self):
        print('There are:', len(self.data), 'entries.')
        process_counter = 0
        for datum in self.data:
            m = self.parse(datum)
            process_counter += 1
            if process_counter % 10 == 0:
                print('Processed:', process_counter, 'of', len(self.data))
        database_ops.insert(self.structured_data)

    def add_to_data(self, unparsed_message):
        # storing the consumer data is the best way to avoid hanging the I/O polling.
        self.data.append(unparsed_message)

    def clear_data(self):
        self.data = {}

if __name__ == "__main__":
    pass
    #p = Parser()
    #p.parse()
    #print('now storing into the database..')
    #database_ops.insert(p.structured_data)
