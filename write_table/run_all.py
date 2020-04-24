import subprocess

tables = [
	'tweets-2019-01',
	'tweets-2019-02',
	'tweets-2019-03',
	'tweets-2019-04',
	'tweets-2019-05',
	'tweets-2019-06',
	'tweets-2019-07',
]

for table in tables:
    year,month = table.split('-')[-2:]
    month = year + month
    print('table:', table)
    subprocess.run(['./run.sh', table, month])
