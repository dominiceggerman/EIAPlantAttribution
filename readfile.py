# Read user creds from text file
def readFile(filename):
    item_list = []
    # Open file and read lines
    with open(filename, mode="r") as infile:
        for line in infile:
            # Strip newline, split, and get item
            item = line.rstrip().split(":")[1]
            # If space at front of username / pass
            if item[0] is " ":
                item = item[1:]
            # Append to cred_list
            item_list.append(item)
    # Return
    return item_list