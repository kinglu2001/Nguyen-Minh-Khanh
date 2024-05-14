#!/usr/bin/python

import string
from datetime import datetime, timedelta
import os

list_trail = [alp+str(num) for alp in list(string.ascii_lowercase) for num in range(0,10)]

# Extract information
prefix_ext_name =  input('Extract name (E_T24 then name will be E_T2401): ').strip().upper()
num_of_thread = input('Anh Cuong muon tao bao nhieu luong??? ')
full_path_dirdat = input('Full path of dirdat (you do not need to create this path, this script will create it): ').strip().rstrip('/')
user_source = input('Database User: ')
sid = input('SID: ')
password = input('Password: ')
schema_source = input('Schema: ').strip().upper()
table_source = input('Table: ').strip().upper()
scn_number = input('SCN Number: ')
partition_column = input('Partition Column: ').upper()
start_date = input("Start date (format like YYYY-MM-DD): ")
step = int(input("Step: "))
# Replicat information
rep_method = input('What method of replicat you want to use? ').strip().lower()
db_target = input('Database Target Name: ').strip().upper()
ip_target = input('IP Target: ').strip()
user_target = input('User In Database Target: ')
password_target = input('Password In Database Target: ')
tb_target = input('Target Table Name: ').strip().upper()

# Create a datetime object for the start_date
# Preprocess datetime
start_date = datetime.strptime(start_date, "%Y-%m-%d")

# Create path dirdat
os.mkdir(full_path_dirdat, mode=0o775)

# generate extract prm file
with open(f'{prefix_ext_name}_ext_prm.txt', 'w') as file:
    for i in range(1, int(num_of_thread)+1):
        next_date = start_date + timedelta(days=step)
        ext_name = prefix_ext_name + str("{:03d}".format(i))
        file.write(
            f'\ncat << EOF > {ext_name}.prm\n'
            f'EXTRACT {ext_name}\n'
            f'EXTFILE {full_path_dirdat}/{list_trail[i-1]}, MEGABYTES 1024, MAXFILES 999\n'
            f'USERID {user_source}@{sid}, PASSWORD {password}\n'
            f'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
            f'TABLE {schema_source}.{table_source}, SQLPREDICATE "AS OF SCN {scn_number} WHERE {partition_column} TO_DATE("{start_date.strftime("%Y-%m-%d")}", "YYYY-MM-DD") AND {partition_column} < TO_DATE("{next_date.strftime("%Y-%m-%d")}", "YYYY-MM-DD")";\n'
            'EOF\n'
        )
        start_date = next_date
    print("-"*40 + f"\nFile {prefix_ext_name}_ext_prm.txt has been created.\n" + "-"*40)
file.close()

# generate add extract cmd
with open(f'{prefix_ext_name}_add.txt', 'w') as file:
    for i in range(1, int(num_of_thread)+1):
        ext_name = prefix_ext_name + str("{:03d}".format(i))
        file.write(
            "\n"*2 + "add exttract " + ext_name + " sourceistable\n"
                    )
    print("-"*40 + f"\nFile {prefix_ext_name}_add.txt has been created.\n" + "-"*40)
file.close()

# generate replicat prm file
# Note:
if rep_method == 'coordinated':
    prefix_rep_name = input('Replicat prefix name (only 2 characters): ').strip().upper()
    thread_range = input('Thread Range (Example: 1-24): ').strip()
    column_list = input('What is your key column? (Example: ID,CREATED_DATE_TIME): ').strip().upper()
    filename = f'{prefix_rep_name}_rep_prm.txt'
    with open(filename, 'w') as file:
        for i in range(1, int(num_of_thread) + 1):
            rep_name = prefix_rep_name + str("{:03d}".format(i))
            file.write(
                f'\ncat << EOF > {rep_name}.prm\n'
                f'REPLICAT {rep_name}\n'
                f'TARGETDB {db_target}@{ip_target}:3306, USERID {user_target}, PASSWORD {password_target}\n'
                f'discardfile ./dirrpt/{rep_name}.dsc, append, megabytes 4000\n'
                f'GROUPTRANSOPS 200000\n'
                f'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
                f'MAP {schema_source}.{table_source}, TARGET {db_target}.{tb_target}, THREADRANGE({thread_range},{column_list});\n'
                'EOF\n'
            )
    print("-"*40 + f"\nFile {filename} has been created.\n" + "-"*40)

    # generate add replicat cmd
    with open(f'{prefix_rep_name}_add.txt', 'w') as file2:
        for i in range(1, int(num_of_thread)+1):
            rep_name = prefix_rep_name + str("{:03d}".format(i))
            file2.write(
                f"\nadd replicat {rep_name} coordinated exttrail {full_path_dirdat}/{list_trail[i-1]}\n"
            )
        print("-"*40 + f"\nFile {prefix_rep_name}_add.txt has been created.\n" + "-"*40)
    file.close()
else:
    prefix_rep_name = input('Replicat prefix name (only 5 characters): ').strip().upper()
    filename = f'{prefix_rep_name}_rep_prm.txt'
    with open(filename, 'w') as file:
        for i in range(1, int(num_of_thread) + 1):
            rep_name = prefix_rep_name + str("{:03d}".format(i))
            file.write(
                f'\ncat << EOF > {rep_name}.prm\n'
                f'REPLICAT {rep_name}\n'
                f'TARGETDB {db_target}@{ip_target}:3306, USERID {user_target}, PASSWORD {password_target}\n'
                f'discardfile ./dirrpt/{rep_name}.dsc, append, megabytes 4000\n'
                f'REPORTCOUNT EVERY 1 SECONDS, RATE\n'
                f'MAP {schema_source}.{table_source}, TARGET {db_target}.{tb_target};\n'
                'EOF\n'
            )
    print("-"*40 + f"\nFile {filename} has been created.\n" + "-"*40)

        # generate add replicat cmd
    with open(f'{prefix_rep_name}_add.txt', 'w') as file2:
        for i in range(1, int(num_of_thread)+1):
            rep_name = prefix_rep_name + str("{:03d}".format(i))
            file2.write(
                f"\nadd replicat {rep_name} exttrail {full_path_dirdat}/{list_trail[i-1]}\n"
            )
        print("-"*40 + f"\nFile {prefix_rep_name}_add.txt has been created.\n" + "-"*40)
    file.close()