import json

def unpack_receipts():
    #get each receipt and loop line items to make transactional table
    lines=[]

    with open("..\\data\\receipts.json", 'r') as file:

        for line in file:

            data = json.loads(line)
            print(data) 
            try:
                receipt_list = data["rewardsReceiptItemList"]
            except:
                print(data)
                pass

            for item in receipt_list:
                line_item = item
                line_item["_id"] = data["_id"]
            lines.append(line_item)

    filename = "..\\data\\receipt_line_item.jsonl"
    with open(filename, 'w') as file:
        for data in lines:

            json_line = json.dumps(data)
            file.write(json_line + '\n')
    
    print("Created receipt line item file as {filename}")