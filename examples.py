import requests
import json

from pprint import pprint as pp

def id(field_name):
    return {
        "email": "field_848",
        "dev": "field_2258",
        "short_description": "field_747",
        "long_description": "field_2258"
    }[field_name]
  


def hits(reponse):
  return response['hits']['hits']

def count(response):
  return reponse['hits']['total']

def took(response):
  return response['took']

def pluck_field(response, field_name):
  hits = hits(response)
  return map(lambda x: x['_source']['doc'].get(field_name, None),
               hits)


  def email_addresses(self, hit_list):
    return map(lambda x: x['_source']['doc'].get('field_848', None),
               hit_list)



class Connection:
  """
  Includes some methods for constructing elasticsearch queries succinctly, as
  well as some example queries that use them.
  """

  def __init__(self,url="http://localhost:9200"):
    self.base_url = url
    self.urls = [] # contains all the urls this object generates

  ### Utility Methods ###

  def path(self, relative_path):
    url = self.base_url + relative_path
    self.urls.append(url)
    return url

  def get(self, relative_path, data=None, verb=requests.get):
    url = self.path(relative_path)
    response = verb(url, data=data)
    return (response.json(), url, response.status_code)

  ### Bulk Insert ###

  ### Example Methods ###

  def get_identity(self):
    return self.get("/")

  def get_entire_mapping(self):
    return self.get("/_mapping")

  def get_mapping_of_type(self, type_name):
    return self.get("/%s/_mapping"
                    %(type_name))

  def tokenize_text(self, text):
    return self.get("/_analyze?analyzer=standard",
                    data=text)

  def full_text_search(self, term, limit=50000):
    return self.get("/apricot/tier1/_search?size=%s&q=%s"
                    %(limit,term))

  def full_text_search_including_tier2s(self, term):
    return self.get("/apricot/tier1,tier2/_search?q=%s"
                    %(term))

  ### Using the URI-based Query API ###

  def first_hit(self, term):
    return self.full_text_search(term)[0]['hits']['hits'][0]['_source']['doc']

  def first_three_hits(self, term):
    return self.full_text_search(term)[0]['hits']['hits'][:3]

  def hits(self, search_result_json):
    return search_result_json['hits']['hits']

  ### Using the Request Body based Query API ###

  def is_valid(self, method):
      return method in ['term',    # precise string match
                        'match',   # substring match (probably)
                        'prefix',  # match start of word
                        'wildcard' # allow * and ?
                       ]

  def search_field(self, field, term, method='wildcard'):
      data = {"query": {method: {field: term }}}
      body = json.dumps(data)
      return self.get("/apricot/tier1/_search?size=50000",
                      data=body)

  def search_email_addresses(self, term):
      return self.search_field("field_848", term)

  def search_short_descriptions(self, term):
      return self.search_field("field_747", term)

  def search_assigned_dev(self, name):
      return self.search_field("field_2258", name)

  def search_long_description(self,term):
      return self.search_field("field_2258", name)

      
  ### Compound Queries ###

  ### Combining Parents with Children ###

  ### Combining Filtering and Searching ###

  ### Arbitrary Subqueries ###

  ### Aggregations ###

  ### Lenses? ###

#  def bulk_insert(self,list_of_json_objects):
#  def match_precise_field_value(self,field_name,value):
#  def full_text_search(self,term):
#  def everything_with_nonnull_value_for_field(self,field_name):
#  def first_created(self,field):
#  def last_created(self,field):
#  filter(lambda x: x != None, conn.email_addresses(conn.hits(conn.full_text_search("*.ca")[0])))
