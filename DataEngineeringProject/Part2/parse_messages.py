import re

class Parser():
    def __init__(self):
        self.data = {}
        self.pattern_keys = {
                "BREADCRUMBS": 
        }


    def parse(self, string):
        fields = string.split(',')
        
        print(data)
    
    def parse_data_fields(self, substring):
        pair = substring.split(':')
        for item in pair:
            item = item.rstrip('"')
            item = item.lstrip('"')


    def __add_to_data(self, parsed_message):
        pass

    def clear_data(self):
        self.data = {}

if __name__ == "__main__":
    p = Parser()
    p.parse()
