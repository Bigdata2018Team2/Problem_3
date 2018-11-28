if __name__ == "__main__":
    import pandas as pd
    import apyori
    import pickle
    import os
    import sys

    print("check dumpfiles...")
    if len(os.listdir("./dumps")) == 0: # if no dump files exist, read csv file and make dumps.
        print("no dump file...")
        print("generate transactions...")

        print("read csv file...")
        data = pd.read_csv("./data/train/train.csv")

        print("drop unnecessary columns...")
        data = data.drop("add_to_cart_order", axis=1)
        data = data.drop("reordered", axis=1)

        print("convert to tuple...")
        data_tuple = data.get_values()

        print("convert data to transaction")
        transactions = {}
        for row in data_tuple:
            if row[0] in transactions:
                transactions[row[0]].append(str(row[1]))
            else:
                transactions[row[0]] = [str(row[1])]
        ids = transactions.keys()
        transactions_values = list(transactions.values())

        # Save transactions to pickle file
        transactions_size = len(transactions_values)
        pickle_len_per_file = 100000
        idx = 0
        while ((idx + 1) * pickle_len_per_file < transactions_size):
            transactions_pickle = open("dumps/transactions.pickle.{}".format(idx), "wb")
            pickle.dump(transactions_values[(idx * pickle_len_per_file):((idx + 1) * pickle_len_per_file)], transactions_pickle)
            transactions_pickle.close()
            print("dump: {}:{}".format(idx * pickle_len_per_file, (idx + 1) * pickle_len_per_file))
            idx += 1
        if (idx * pickle_len_per_file < transactions_size):
            transactions_pickle = open("dumps/transactions.pickle.{}".format(idx), "wb")
            pickle.dump(transactions_values[(idx * pickle_len_per_file):], transactions_pickle)
            transactions_pickle.close()
            print("dump: {}:{}".format(idx * pickle_len_per_file, transactions_size))
    else: # if dump files exist, load them
        print("dump file found")
        print("restructuring transactions")
        dump_file_count = len(os.listdir("dumps"))
        transactions_values = []
        for i in range(dump_file_count):
            print("load transactions.pickle.{}".format(i))
            transactions_pickle = open("dumps/transactions.pickle.{}".format(i), "rb")
            transactions_values += pickle.load(transactions_pickle)
            transactions_pickle.close()

    print("counting items")
    item_count = dict()
    for i in range(len(transactions_values)):
        for j in range(len(transactions_values[i])):
            cnt = 1
            item = str(transactions_values[i][j])
            if item in item_count:
                cnt += item_count.get(item)
            item_count[item] = cnt

    # min_support = 0.005
    # min_confidence = 0.2
    min_support, min_confidence = list(map(float, sys.argv[1:3]))
    print("make transaction rules wiht min_support={}, min_confidence={}".format(min_support, min_confidence))
    association_rule = apyori.apriori(transactions_values, min_support=min_support, min_confidence=min_confidence, min_lift=3, min_length=2)
    association_result = list(association_rule)
    print("Association rule: {}".format(len(association_result)))

    print("result===================")
    cnt = 1
    for item in association_result:
        items = [i for i in item.items]
        print(cnt)
        print("Rule:\t" + items[0] + " -> " + items[1])
        print(items[0],'\'s tractions:',item_count[items[0]])
        print("Support:\t",item.support)
        print("Confidence:\t", item.ordered_statistics[0][2])
        print("Lift:\t", item.ordered_statistics[0][3])
        print('==============================================')
        cnt += 1

    rule_pickle = open("rules/rules({}).{}-{}.pickle".format(cnt - 1, min_support, min_confidence), "wb")
    pickle.dump(association_result, rule_pickle)
    rule_pickle.close()

    # transactions_file = open("./transactions.txt", "w")
    # ids_file = open("./ids", "w")
    
    # print("writting start...")
    # for id in ids:
    #     transactions_file.write(",".join(transactions[id]) + "\n")
    #     ids_file.write("{}\n".format(id))
    
    # print("done.")
    # ids_file.close()
    # transactions_file.close()