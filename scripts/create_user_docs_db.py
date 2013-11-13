#!/usr/bin/env python
#
# Create the user_docs DB in CouchDB, which holds user-specific document (i.e. their guesses for various hints)
#

import requests, json

URL = 'http://localhost:5984/user_docs'

design_documents = [
{
  '_id'   : '_design/popular_guesses',\
  'views' : {
    'popular_guesses' : {
      'language' : 'javascript',
      'map'    : '''
      function(doc) {
        Object.keys(doc.guesses).forEach(function(key){
          var guess = doc.guesses[(key)];
          if (guess) {
            emit([key, guess], null);
          }
        });
      }''',
      'reduce' : '_count'
    }
  }
},
{
  '_id'  : '_design/app',\
  'filters': {
    'by_user' : '''
       function(doc, req) {
           return doc._id === req.userCtx.name;
       }
    '''
  }
}, 
{
  '_id' : '_design/validate_correct_user',\
  'validate_doc_update' : '''
    function(new_doc, old_doc, userCtx) {
      if (!userCtx.name || !new_doc._id || new_doc._id !== userCtx.name) {
        throw({forbidden : "Not authorized! Your userdoc must have the same id as your username."});
      }
      
      if (!new_doc._deleted) { // users can always delete
        if (!new_doc.guesses || typeof new_doc.guesses !== 'object'){
          throw({forbidden : "Not authorized! Your userdoc must have an object map for its guesses."});
        } 
        if (Object.keys(new_doc.guesses).length > 100000) {
          throw({forbidden : "That's a huge object!  You're probably trying to crash this server."});
        }

        Object.keys(new_doc.guesses).forEach(function(key) {
          if (typeof key !== 'string') {
            throw({forbidden : "Guesses must be a basic string-to-string map."});
          }
          if (key.length > 1000) {
            throw({forbidden : "That's a huge key!  You're probably trying to crash this server."});
          }
          var value = new_doc.guesses[(key)];
          if (typeof value !== 'string' && value !== null) {
            throw({forbidden : "Guesses must be a basic string-to-string map."});
          }
          if (value && value.length > 1000) {
            throw({forbidden : "That's a huge value!  You're probably trying to crash this server."});
          }
        });
      }

    }
  '''
}
]


print 'dropping database %s, response is %s' % (URL, requests.delete(URL).status_code)
print 'creating database %s, response is %s' % (URL, requests.put(URL).status_code)

for design_doc in design_documents:
  response = requests.put(URL + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
  print 'posted design doc %s to CouchDB, got response %d %s' % (design_doc['_id'], response.status_code, response.json())
