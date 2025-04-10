import pandas as pd

tsv_file = "cskg.tsv"
output_file = "import.surql"
table_name = "test"

df = pd.read_csv(tsv_file, sep="\t", dtype=str, low_memory=False, on_bad_lines='skip')

df.drop(columns=["id", "relation;dimension", "source", "sentence"], inplace=True)
df.rename(columns={"node1;label": "node1_label", "node2;label": "node2_label", "relation;label": "relation_label"}, inplace=True)

print(f"Loaded {df.shape[0]} rows from {tsv_file}")

queries = []
for i, row in df.iterrows():
    formatted_fields = []
    for col in df.columns:
        value = str(row[col])

        if '\\' in value:
            value = value.replace('\\', '') # idk if this is alright

        if '"' in value:
            value = value.replace('"', '\\"') # why

        formatted_fields.append(f'{col}: "{value}"')

    fields = ", ".join(formatted_fields)

    # if i == 79371:
    #     print(fields)

    queries.append(f"INSERT INTO {table_name}  {{ {fields} }};")

    if i % 100000 == 0:
        print(f"{i / df.shape[0] * 100:.2f}%")

print(f"Generated {len(queries)} queries")

with open(output_file, "w") as f:
    for i, query in enumerate(queries):
        f.write(query + "\n")

        if i % 1000000 == 0:
            print(f"{i / len(queries) * 100:.2f}%")

print(f"Generated {len(queries)} queries in {output_file}")