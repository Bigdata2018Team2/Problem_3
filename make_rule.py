import pandas as pd
import apyori
import pickle
import os
import sys
from tqdm import tqdm
import threading

class Print_Both:
    def __init__(self, file_name, overwrite=False):
        self.file_name = file_name
        if overwrite:
            mode = "w"
        else:
            mode = "a"
        self.file = open(file_name, mode)

    def print(self, string, to_stdout=True, to_file=True):
        if to_stdout:
            print(string)
        if to_file:
            self.file.write("{}".format(string))

    def close(self):
        self.file.close()

global association_result

def recommendation(transaction):
    cnt = 1
    result = list()
    transaction_set = set(transaction)
    for item in association_result:
        for rule in item.ordered_statistics:
            base = set(rule.items_base)
            add = list(rule.items_add)
            if base.issubset(transaction_set):
                result.append((add, rule.confidence))
    return sorted(result, reverse=True, key=lambda a: a[1])


if __name__ == "__main__":
    
    if len(sys.argv) < 4:
        print("Usage: {} <file_name> <min_support> <min_confidence>".format(sys.argv[0]))
    
    file_path = sys.argv[1]
    file_name = os.path.basename(file_path)
    dump_path = os.path.join(os.getcwd(), "dumps/{}.dumps".format(file_name))
    result_path = os.path.join(os.getcwd(), "result/{}.result".format(file_name))
    rules_path = os.path.join(os.getcwd(), "rules/{}.result".format(file_name))

    print("check dumpfiles...")
    if not os.path.isdir(dump_path):
        os.mkdir(dump_path)

    if len(os.listdir(dump_path)) == 0: # if no dump files exist, read csv file and make dumps.
        print("no dump file...")
        print("generate transactions...")

        print("read csv file...")
        data = pd.read_csv(file_path)

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
            transactions_pickle = open("{}/transactions.pickle.{}".format(dump_path, idx), "wb")
            pickle.dump(transactions_values[(idx * pickle_len_per_file):((idx + 1) * pickle_len_per_file)], transactions_pickle)
            transactions_pickle.close()
            # print("dump: {}:{}".format(idx * pickle_len_per_file, (idx + 1) * pickle_len_per_file))
            idx += 1
        if (idx * pickle_len_per_file < transactions_size):
            transactions_pickle = open("{}/transactions.pickle.{}".format(dump_path, idx), "wb")
            pickle.dump(transactions_values[(idx * pickle_len_per_file):], transactions_pickle)
            transactions_pickle.close()
            # print("dump: {}:{}".format(idx * pickle_len_per_file, transactions_size))
    else: # if dump files exist, load them
        print("dump file found")
        print("restructuring transactions")
        dump_file_count = len(os.listdir(dump_path))
        transactions_values = []
        for i in tqdm(range(dump_file_count)):
            transactions_pickle = open("{}/transactions.pickle.{}".format(dump_path, i), "rb")
            transactions_values += pickle.load(transactions_pickle)
            transactions_pickle.close()

    # print("counting items")
    # item_count = dict()
    # for i in range(len(transactions_values)):
    #     for j in range(len(transactions_values[i])):
    #         cnt = 1
    #         item = str(transactions_values[i][j])
    #         if item in item_count:
    #             cnt += item_count.get(item)
    #         item_count[item] = cnt

    # min_support = 0.005
    # min_confidence = 0.2
    min_support, min_confidence = list(map(float, sys.argv[2:4]))
    print("check rule dump...")
    if not os.path.isdir(rules_path):
        os.mkdir(rules_path)
    if os.path.isfile("{}/rules.{}-{}.pickle".format(rules_path, min_support, min_confidence)):
        print("rule already exists...")
        print("restore rule")
        rule_pickle = open("{}/rules.{}-{}.pickle".format(rules_path, min_support, min_confidence), "rb")
        association_result = pickle.load(rule_pickle)
        rule_pickle.close()
        print("load rules: {}".format(len(association_result)))
    else:
        print("dump file doesn't exist")
        print("make association rules wiht min_support={}, min_confidence={}".format(min_support, min_confidence))
        association_rule = apyori.apriori(transactions_values, min_support=min_support, min_confidence=min_confidence)
        association_result = list(association_rule)
        print("Association rule: {}".format(len(association_result)))

        print("result===================")
        cnt = 1
        for item in association_result:
            items = [i for i in item.items]
            print(cnt)
            print(items)
            # print(items[0],'\'s tractions:',item_count[items[0]])
            print("Support:\t",item.support)
            print("Confidence:\t", item.ordered_statistics[0][2])
            print("Lift:\t", item.ordered_statistics[0][3])
            print('==============================================')
            cnt += 1

        rule_pickle = open("{}/rules.{}-{}.pickle".format(rules_path, min_support, min_confidence), "wb")
        pickle.dump(association_result, rule_pickle)
        rule_pickle.close()

    # Recommandation start
    print("Recommandation start...")
    if not os.path.isdir(result_path):
        os.mkdir(result_path)
    pb = Print_Both("{}/recommandation.{}-{}.txt".format(result_path, min_support, min_confidence), overwrite=True)
    cnt = 1
    transactions_size = len(transactions_values)
    print("     ", end='')
    for transaction in tqdm(transactions_values):
        result = recommendation(transaction)
        items = list()
        for r in result:
            items += r[0]
        items = set(items)
        transaction_set = set(transaction)
        pb.print("[{}]".format(len(items)) + ",".join(list(map(str, list(items.difference(transaction_set))[:5]))), to_stdout=False)
    
    # num_of_cores = 4
    # for thread_num in range(num_of_cores):
    #     threading.Thread(target=lambda )
        
    pb.close()
    
