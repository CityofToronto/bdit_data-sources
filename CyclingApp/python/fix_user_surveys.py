# coding: utf-8
'''Quick python script to pad each line of a csv to the correct number of lines'''

def fix_line(line):
    line_count = line.count(',')
    if line_count < 13:
        num_commas = 13 - line_count
        l_array = line.split("\n")
        l_array[0] += "," * num_commas + "\n"
        return l_array[0]
    else:
        return line
    
if __name__ == '__main__':
    with open('user_surveys.csv', 'r') as oldfile, open('user_surveys_fixed.csv', 'w') as newfile:
            for line in oldfile.readlines():
                newfile.write(fix_line(line))