import re
import database_ops
import time

class Parser():
    def __init__(self):
        self.data = []
        self.pattern_keys = {
                # This was just a bout of laziness
                "BREADCRUMBS": r'(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+-\w+-\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+)[\"|\s|\,|\w]+: "(\d+|)[\"|\s|\,|\w]+: "(\d+|)[\"|\s|\,|\w]+: "(\d+|)[\"|\s|\,|\w]+: "(-\d+.\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(-\d+.\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(\d+|)[\"|\s|\,|\w]+: "(\d+|\d+.\d+|)[\"|\s|\,|\w]+: "(-\d+|\d+|)"'
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
        """
        Uses the world's ugliest regex string above to filter for matches.
        """
        matches = re.findall(self.pattern_keys["BREADCRUMBS"], string)
        # After using the world's ugliest regex string I presumably have all of the breadcrumb data in-order.
        try:
            self.sort_match_data(matches[0])
        except IndexError:
            pass
            print('NO MATCHES!!')
            print(string)

    def determineTime(self, seconds):
        secs = seconds % 60
        mins = seconds/60
        hrs = mins/60
        # now clean them up
        mins = int(mins % 60)
        hrs = int(hrs % 24)
        str_secs = str(secs)
        str_mins = str(mins)
        str_hrs = str(hrs)
        if secs < 10:
            str_secs = '0'+str(secs)
        if mins < 10:
            str_mins = '0'+str(mins)
        if hrs < 10:
            str_hrs = '0'+str(hrs)
        
        formattedTime = str_hrs+':'+str_mins+':'+str_secs
        return formattedTime

    def sort_match_data(self, matches):
        """
        Appends a piece of new data to it's 'bin'. For example, an event ID would be appended to the end of the event ID dictionary list.
        """
        for i in range(len(self.keys)):
            temp = self.structured_data.get(self.keys[i])
            try:
                if self.keys[i] == 'OPD_DATE':
                    adjusted_opd = str(matches[i]) + ' ' + self.determineTime(int(matches[5])) # 5 is time in seconds
                    temp.append(adjusted_opd)
                else:
                    temp.append(matches[i])
            except IndexError:
                temp.append("")
            self.structured_data.update({self.keys[i] : temp})
    
    def parse_data_fields(self, stop_events=None):
        """
        Data is assumed to already exist in the object. (add_to_data accomplishes this)
        Existing data behaves like a buffer, and the parsing tool gradually processes the list of data in entirety.
        Data is consumed in a json-like format.
        """
        print('There are:', len(self.data), 'entries.')
        process_counter = 0
        for datum in self.data:
            self.parse(datum)
            process_counter += 1
            if process_counter % 1000 == 0:
                print('Processed:', process_counter, 'of', len(self.data))
        if stop_events:
            database_ops.insert(self.structured_data, stop_events)
        else:
            database_ops.insert(self.structured_data)

    def add_to_data(self, unparsed_message):
        # storing the consumer data is the best way to avoid hanging the I/O polling.
        self.data.append(unparsed_message)

    def clear_data(self):
        self.data = {}

if __name__ == "__main__":
    print('debug statements here')
    pass
