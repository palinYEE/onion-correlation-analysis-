# 2021-01-06

from elasticsearch import Elasticsearch

class Elastic():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

        self.es = Elasticsearch(f"{self.ip}:{self.port}", timeout=120, max_retries=10, retry_on_timeout=True)

    def searchData(self, index, query, size):
        if size >= 10000:
            return self.es.search(index = index, body = query, size=size,  scroll = '1m')  
        else:
            return self.es.search(index = index, body = query, size = size)

    def scrollData(self, scrollId):
        return self.es.scroll(scroll_id=scrollId, scroll='1m')

    def clearScroll(self, scrollId):
        self.es.clear_scroll(scroll_id=scrollId)

    def multisearchData(self, query):
        return self.es.msearch(body = query)

def Onion_Search(mainfile_ES):
    size = 10000
    searchOnion = input("검색할 onion 주소를 입력하세요 : ")
    if 'http' in searchOnion : searchOnion.replace('http://', '')
    ESquery = {"query": {"bool": {"filter": {"term": {"onion" : searchOnion}}}}}
    result = mainfile_ES.searchData(index = 'mainfile-*', query = ESquery, size = size)
    try:
        scrollId = result['_scroll_id']
        while result['hits']['hits']:
            mainfile_ES.scrollData(scrollId = scrollId)
            mainfile_ES.clearScroll(scrollId = scrollId)
    except Exception as e: pass
    return result['hits']['hits']

def data_refine(ESdata:dict):
    result_dic = {
        "onion" : ESdata[0]["_source"]["onion"],
        "component" : [
            {
                "file_name" : data["_source"]["file_name"],
                "file_path" : data["_source"]["file_path"],
                "hash"      : data["_source"]["hash"],
                "timeUsed"  : data["_source"]["timeUsed"]
            } for data in ESdata
            ] 
    }
    return result_dic

def correlation_data(mainfile_ES, ESdata_dic:dict):
    msearch_query = []
    correlation = []

    for data in ESdata_dic["component"]:
        msearch_query.append({"index": 'mainfile-*'})
        msearch_query.append({"query": {"bool": {"filter": {"term": {"hash" : data["hash"] }}}}})
    
    result = mainfile_ES.multisearchData(query = msearch_query)
    for response in result['responses']:
        for hits in response['hits']['hits']:
            correlation.append(hits["_source"]["onion"])

    return list(set(correlation))
    
def correlation_percentation(mainfile_ES, onionlist:list, search_onion_data:dict):
    msearch_query = []
    result = {
        "search_main_onion" : search_onion_data['onion'],
        "correlation" : []
    }
    
    for onion in onionlist:
        msearch_query.append({"index": "mainfile-*"})
        msearch_query.append({"query": {"bool": {"filter": {"term": {"onion" : onion}}}}})

    msearch_result = mainfile_ES.multisearchData(query = msearch_query)
    # print(msearch_result)
    onion_data = []
    for response in msearch_result["responses"]: onion_data.append(data_refine(response['hits']['hits']))

    correlation_list = []
    for compare_data in onion_data:
        hash_list = [data['hash'] for data in compare_data['component']]
        count = 0
        for search_hash in search_onion_data['component']:
            if search_hash['hash'] in hash_list:
                count += 1
        correlation_list.append({"onion" : compare_data['onion'],"correlation_percent" : (count/len(hash_list))*100})
        # result['correlation'].append(correlation)

    res = sorted(correlation_list, key=lambda percentage: percentage['correlation_percent'], reverse=True)
    result["correlation"] = res
    # result['correlation'].sort(key = result['correlation']['correlation_percent'] ,reverse = True)
    return result

    # print(onion_refine_data)
    # for onion_data in onion_refine_data:

if __name__ == "__main__":
    mainfile_ES = Elastic(ip = '192.168.99.100', port='9200')
    onion_mainfile_data = Onion_Search(mainfile_ES=mainfile_ES)
    refine_data = data_refine(ESdata = onion_mainfile_data)
    correlation_onion = correlation_data(mainfile_ES=mainfile_ES, ESdata_dic=refine_data)
    correlation_percentage = correlation_percentation(mainfile_ES=mainfile_ES, onionlist=correlation_onion, search_onion_data=refine_data)
    print(f'[*] {correlation_percentage["search_main_onion"]} 과 연관되어 있는 onion 주소 목록')
    for correlation in correlation_percentage['correlation']:
        print(f' [-] {correlation["onion"]} --> {correlation["correlation_percent"]}%')
    # print(correlation_onion)
    pass