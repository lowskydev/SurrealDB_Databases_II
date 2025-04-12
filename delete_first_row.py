with open('target.tsv', 'r', encoding='utf-8') as infile, open('part1.tsv', 'w', encoding='utf-8') as outfile:
    next(infile)  # Skip the first line
    for line in infile:
        outfile.write(line)

print("Done")