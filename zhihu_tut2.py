import requests
import re
import time
import json
import random
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession

from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random

headers = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
}

proxies = []

ua = UserAgent(verify_ssl=False)

def getproxies():
    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', ua.random)
    proxies_doc = urlopen(proxies_req).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
            'ip':   row.find_all('td')[0].string,
            'port': row.find_all('td')[1].string
        })


def random_proxy():
    return random.randint(0, len(proxies) - 1)


# link to questions on page @offset under topic @tpid
def getTopicUrlEssence(tpid, offset):
    return "http://www.zhihu.com/api/v4/topics/{}/feeds/essence?include=data%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.content%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.is_normal%2Ccomment_count%2Cvoteup_count%2Ccontent%2Crelevant_info%2Cexcerpt.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Darticle%29%5D.target.content%2Cvoteup_count%2Ccomment_count%2Cvoting%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Dpeople%29%5D.target.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Canswer_type%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.paid_info%3Bdata%5B%3F%28target.type%3Darticle%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dquestion%29%5D.target.annotation_detail%2Ccomment_count%3B&limit=10&offset={}".format(tpid, offset)


# link to answers on page @offset of question
def getAnsUrl(qid, offset):
    # qid: question id
    # offset: page number
    '''
    url = "https://www.zhihu.com/api/v4/questions/{}/answers?include=content&limit=20&offset={}&platform=desktop&sort_by=default".format(qid, offset)
                          https://www.zhihu.com/api/v4/questions/{}/answers?include=data[*].is_normal,admin_closed_comment,reward_info,is_collapsed,annotation_action,annotation_detail,collapse_reason,is_sticky,collapsed_by,suggest_edit,comment_count,can_comment,content,editable_content,voteup_count,reshipment_settings,comment_permission,created_time,updated_time,review_info,relevant_info,question,excerpt,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp,is_labeled,is_recognized,paid_info,paid_info_content;data[*].mark_infos[*].url;data[*].author.follower_count,badge[*].topics&limit=20&offset={}&platform=desktop&sort_by=default .format(qid, offset)
    fullest infos: url = "https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&platform=desktop&sort_by=default".format(qid, offset)
    '''
    # its necessary to slim down the datas
    # currently we removed these from fullest_info: question, excerpt, relevant_info, paid_info, paid_info_content, suggest_edit, relationship.is_authorized, is_author,voting,is_thanked,is_nothelp,is_labeled,is_recognized
    #        https://www.zhihu.com/api/v4/questions/{}/answers?include=data[*].is_normal,admin_closed_comment,reward_info,is_collapsed,annotation_action,annotation_detail,collapse_reason,is_sticky,collapsed_by,comment_count,can_comment,content,editable_content,voteup_count,reshipment_settings,comment_permission,created_time,updated_time,review_info;data[*].mark_infos[*].url;data[*].author.follower_count,badge[*].topics&limit=20&offset={}&platform=desktop&sort_by=default .format(qid, offset)
    return  "https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&platform=desktop&sort_by=default".format(qid, offset)


# Save infos about question to a file
def finalsave(listAns, interrupted, que, qid):
    # listAns: list of answers
    # itnerrupted: if the crawling is interrupted, i.e. if the crawling of question is complete
    # que: info about question
    # qid: id of question
    # REQUIRE listAns.len > 0

    if len(listAns) > 0:
        thisQuest = {
            'question': que,
            'answers': listAns,
            'completed': not interrupted
        }
    else:
        raise Exception("No answer for the question crawled")
    with open('answers/'+str(qid)+'.json', 'w', encoding='utf-8') as f:
        json.dump(thisQuest, f, ensure_ascii=False, indent=4)


# Simplify datas
def slimdown(answer):
    del answer['question'] #repitition
    del answer['author']['badge'] #unnecessary
    del answer['author']['badge_v2'] #unnecessary
    answer['is_rewardable'] = answer['reward_info']['is_rewardable']
    del answer['reward_info'] #unnecessary
    del answer['relationship']
    del answer['is_copyable']
    del answer['extras']
    del answer['is_normal']
    del answer['is_sticky']
    del answer['is_collapsed']
    del answer['collapsed_by']
    del answer['collapse_reason']

    return answer

    
# Use zhihu api to get json data about the question
def getQuestionDetail(qid, proxy):
    # qid: question id

    url = "https://www.zhihu.com/api/v4/questions/{}?include=answer_count".format(qid)
    res = requests.get(url, headers = headers, proxies = proxy)
    res.encoding = 'utf-8'
    return res.json()


# Use zhihu api to get json data about the answers, singlethread
def getWebpage(qid, offset, proxy):
    # qid: question id
    # offset: page number
    h = {'User-Agent': ua.random}
    res = requests.get(getAnsUrl(qid, offset), headers=h, proxies = proxy)
    res.encoding = 'utf-8'
    return res.json()


# Get answers to question @qid, singlethread
def getQAnswers(qid):
    # qid: qustion id
    interrupted = False
    offset = 0
    num = 0
    anses = list()

    proxy_index = random_proxy()
    proxy = proxies[proxy_index]

    que = getQuestionDetail(qid, proxy)
    ansLen = que
    number_of_answers = que['answer_count']
    n = 0
    while True:
        if n % 10 == 0:
            proxy_index = random_proxy()
            proxy = proxies[proxy_index]
            print("proxy changed to: " + str(proxy))
        # zhihu api request
        try:
            data = getWebpage(qid, offset, proxy)
            for line in data['data']:
                # save answers
                anses.append(slimdown(line))
            print(str(offset) + '/' + str(number_of_answers) +' done')

            if data['paging']['is_end']: # if reaches the end
                print("reach the end of answers")
                break
            if len(data['data']) == 0 and not data['paging']['is_end']: # if request fails
                print("crawling failed, ip might be flagged")
                interrupted = True
                break

            offset += 20
            n += 1
        except:
            print('Proxy ' + proxy['ip'] + ':' + proxy['port'] + ' deleted.')
            proxy_index = random_proxy()
            proxy = proxies[proxy_index]

        time.sleep(random.randint(0, 200)/1000)
        #time.sleep(random.randint(200, 500)/1000) if ip been banned

    if len(anses) > 0:
        finalsave(anses, interrupted, que, qid)
    else:
        print("No answer for the question " + str(qid) + " crawled")


# Use zhihu api to get json data about the answers, multithread, returning a generator
def getWebpages(qid, start, end, workernum = 8, top = 1000):
    # qid: question id
    # start: crawl from question number @start     
    # end: number of answers in this question
    # workernum: multithread worker number 
    current_offset = start

    with FuturesSession(max_workers = workernum) as session:
        while current_offset < end and current_offset <= top:
            proxy_index = random_proxy()
            proxy = proxies[proxy_index]
            numr = workernum
            futures= []
            for o in range(current_offset, min(current_offset+numr*20, end), 20):
                h = {'User-Agent': ua.random}
                futures.append(session.get(getAnsUrl(qid, o), headers=h, proxies = proxy))
            current_offset += numr*20
            for future in as_completed(futures):
                resp = future.result()
                if resp is not None:
                    resp.encoding = 'utf-8'
                    resp = resp.json()
                    if 'data' in resp:
                        yield resp
                    else:
                        print("failed crawling of one segment")
                else: 
                    print("reaches end of answer crawling or problem with crawling")
            time.sleep(1)

# Get answers to question @qid, multithread
def getQAnswersMulti(qid):
    # qid: question id
    interrupted = False
    offset = 0
    num = 0
    anses = list()
    proxy_index = random_proxy()
    proxy = proxies[proxy_index]

    que = getQuestionDetail(qid, proxy)
    number_of_answers = que['answer_count']

    datas = getWebpages(qid, 0, number_of_answers, 8)

    prog = 0
    for data in datas:
        if len(data['data']) != 0:
            for line in data['data']:
                anses.append(slimdown(line))
                prog += 1
        else:
            interrupted = True
            print("crawling failed partially, ip might be flagged")
        print(str(prog) + "/" + str(number_of_answers) + " done")

    if len(anses) > 0:
        finalsave(anses, interrupted, que, qid)
    else:
        print("No answer for the question " + str(qid) + " crawled")


def getQuesIds(tpid, start = 0, sortby = 'essence', end = 1000, limit = 10):

    proxy_index = random_proxy()
    proxy = proxies[proxy_index]

    if sortby == 'essence':
        current_offset = start
        while current_offset < end:
            h = {'User-Agent': ua.random}
            resp = requests.get(getTopicUrlEssence(tpid, current_offset), headers = h, proxies = proxy)
            resp.encoding = 'utf-8'
            resp = resp.json()
            print(resp)
            if 'data' in resp:
                for data in resp['data']:
                    yield data['target']['question']['id']
                current_offset += limit
            else:
                del proxies[proxy_index]
                print('Proxy ' + proxy['ip'] + ':' + proxy['port'] + ' deleted.')
                proxy_index = random_proxy()
                proxy = proxies[proxy_index]
                print("switch proxy")


# Get question ids under topic @tpid, return a generator
def getQuesIdsMulti(tpid, start = 0, sortby = 'essence', end = 1000, workernum = 8, limit = 10):
    if sortby == 'essence':
        current_offset = start
        with FuturesSession(max_workers = workernum) as session:
            while current_offset < end:
                numr = random.randint(1, workernum+1)
                futures = [session.get(getTopicUrlEssence(tpid, o), headers=headers, proxies = proxies) for o in range(current_offset, min(current_offset+numr*limit, end), limit)]
                current_offset += numr*limit
                for future in as_completed(futures):
                    resp = future.result()
                    resp.encoding = 'utf-8'
                    resp = resp.json()
                    if 'data' in resp:
                        for data in resp['data']:
                            yield data['target']['question']['id']
                    else:
                        print("reaches end of crawling question id or problem with crawling")
                time.sleep(1)


if __name__ == '__main__':
    getproxies()
    # root topic
    count = 1
    for i in getQuesIds(19776749, count):
        print(str(i) + ' ' + str(count))
        getQAnswersMulti(i)
        print(str(i) + ' ' + str(count)+" finished")
        count += 1
    

