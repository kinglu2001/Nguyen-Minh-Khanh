#!/usr/bin/python

import string
list_trail = [alp+str(num) for alp in list(string.ascii_lowercase) for num in range(0,9) ]

user_id_alias = input("Input your useridalias here: ")

# Read file
f = open("table_topics.txt", "r")
tables = f.read()
ext_table = tables.split("---")

# generate extract prm file
with open('output.txt', 'w') as file:
    for i in range(1, len(ext_table)+1):
        file.write("\n\n\ncat << EOF > " + "E_T24"+ str("{:03d}".format(i)) +
                ".prm\nEXTRACT " + "E_T24"+ str("{:03d}\n".format(i)) +
                "SETENV (TNS_ADMIN = '/oracle/app/oracle/product/19c/dbhome_1/network/admin')" + 
                f"\nUSERIDALIAS {user_id_alias}" + 
                "\nDBOPTIONS ALLOWUNUSEDCOLUMN"
                "\nEXTFILE ./dirdat/"+ list_trail[i] + 
                "\n" +
                ext_table[i-1] +
                "EOF")
file.close()
    #print('E_T24'+ str("{:03d}".format(start_ext)))


# generate add extract cmd
with open('add_ext.txt', 'w') as file:
    for i in range(1, len(ext_table)+1):
        file = open("add_ext.txt", "a")
        file.write("\n"*3 + 
                    "add exttract "  +
                    "E_T24"+ str("{:03d}".format(i)) + " sourceistable")
file.close()
