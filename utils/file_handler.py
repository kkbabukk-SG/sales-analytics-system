def read_sales_data(filename):
    print("--- File content (line by line) ---")

    encodings = ["utf-8", "cp1252", "latin-1"]

    for enc in encodings:
        try:
            with open(filename, "r", encoding=enc) as f:
                # Skip header row
                header = next(f, None)

                for number, line in enumerate(f, start=1):
                    line = line.strip()

                    # Skip empty lines
                    if not line:
                        continue

                    print(f"{number}: {line}")
            break

        except UnicodeDecodeError:
            continue

        except FileNotFoundError:
            print(f"Error: File not found → {filename}")
            return

    else:
        print("Failed to decode file with known encodings.")


# Call the function
filename = "C:\Masai project\data\sales_data.txt"
read_sales_data(filename)
