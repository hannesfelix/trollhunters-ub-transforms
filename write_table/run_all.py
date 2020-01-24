import subprocess

tables = [
    'tweets-2018-01',
    'tweets-2018-02',
    'tweets-2018-03',
    'tweets-2018-04',
    'tweets-2018-05'
]

for table in tables:
    year,month = table.split('-')[-2:]
    month = year + month
    print('table:', table)
    subprocess.run(['./run.sh', table, month])
