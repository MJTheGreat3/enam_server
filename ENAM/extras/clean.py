import pandas as pd
import re

def clean_name(name):
    """
    Removes variants of LTD / LIMITED / L / LIMITE case-insensitively,
    including any trailing dots.
    """
    # Match the words case-insensitively and remove trailing dots
    pattern = r'\b(LTD\.?|LIMITED|LIMITE|L|LT\.?)\b\.?'
    cleaned = re.sub(pattern, '', name, flags=re.IGNORECASE)
    # Collapse multiple spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def main():
    input_file = "stock.csv"
    output_file = "stock.csv"

    print(f"[INFO] Loading {input_file}...")
    df = pd.read_csv(input_file)

    if not {'Symbol', 'Name'}.issubset(df.columns):
        print("[ERROR] CSV must have columns 'Symbol' and 'Name'")
        return

    print("[INFO] Generating Alias column...")
    aliases = []
    for _, row in df.iterrows():
        symbol = str(row['Symbol']).strip()
        name = str(row['Name']).strip()
        cleaned = clean_name(name)
        alias = f"{symbol}|{name}|{cleaned}"
        aliases.append(alias)

    df['Alias'] = aliases

    print(f"[INFO] Saving to {output_file}...")
    df.to_csv(output_file, index=False)
    print("[SUCCESS] Alias column added and saved.")

if __name__ == "__main__":
    main()
