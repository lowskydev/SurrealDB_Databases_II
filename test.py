# import pandas as pd
# import csv

# csv_file = "part1.tsv"

# df = pd.read_csv(
#         csv_file,
#         sep="\t",
#         dtype=str,
#         quoting=csv.QUOTE_NONE,
#         low_memory=False
#     )

my_set = set()
my_set.add('a')
my_set.add('b')
print(my_set)

# with open(csv_file, 'r', encoding='utf-8') as f:
#     for i, line in enumerate(f):
#         if len(line.split('\t')) != 10:
#             print(f"Line {i} does not have 10 columns: {line.strip()}")

#         if i == 527976:  # zero-based index
#             print(line)
#             print(len(line.split('\t')))