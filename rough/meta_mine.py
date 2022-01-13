from google.cloud import storage
from google.cloud import firestore
from google.cloud import storage
import logging
from datetime import datetime
import os, sys, tempfile

def firestore_load(request):  #request aregument is passed with http & even with event-based
    """request_json = request.get_json()   #Using request args
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Hello World!'"""

    ''' Input for Test
    {
        "message":"Hello lets see if it works",
        "Bucket_name":"gcp_test_csv",
        "File_name": "airtravel.csv",
        "Collection" : "gcp_test_meta",
        "Document" : "airtravel"
    }
    
    Encountered error:
    1> Wrong Project ID in Firestore Client
    2> Split Line of CSV using "\ n" (without space)
 '''


    request_json = request.get_json()
    BUCKET_NAME = request_json["Bucket_name"] #Bucket name
    print(request_json)
    BLOB_NAME=request_json["File_name"]  # File name
    storage_client = storage.Client() #Used for Buckets
    metadata_store = firestore.Client(project="poc-dna-gcp-cdp-623320") #Firestore client
    data_rows=[]
    file_name = storage_client.get_bucket(BUCKET_NAME).blob(BLOB_NAME)
    str_data = file_name.download_as_string()
    str_data_utf8 = str(str_data,"utf-8")
    print(str_data_utf8)
    print("Converting into List")
    list_rows = str_data_utf8.split("\n")
    print("Length : " , len(list_rows))


    header=None
    for elem in list_rows:
        temp=elem.split(",")
        if(temp[0] == "\"Month\""):
            header = elem
            break

    print("Printing Header")

    print(header)

    print("Creating Dictionary")

    header=header.split(",")

    #Creating Dictionary to store in firestore
    for row in list_rows[1:]:
        obj={
            'metadata_file_bucket' : str(BUCKET_NAME),
            'metadata_file_name' : str(BLOB_NAME)
        }
        row_data=row.split(',')
        if (row_data == ['']):
            continue
        for idx,item in enumerate(row_data):
            obj[header[idx]]=item
        data_rows.append(obj)

    #print(data_rows)

    #Storing in FireStore
    """
    # Add a new document
    db = firestore.Client()
    doc_ref = db.collection(u'users').document(u'alovelace')
    doc_ref.set({
        u'first': u'Ada',     ##u'string' means string is in unicode format
        u'last': u'Lovelace',
        u'born': 1815
    })
    
    #  Reading a collection
    doc_ref = db.collection(u'cities').document(u'SF')

    doc = doc_ref.get()
    if doc.exists:
        print(f'Document data: {doc.to_dict()}')
    else:
        print(u'No such document!')
    """

    doc_ref = metadata_store.collection(request_json["Collection"]).document(request_json["Document"])
    for elem in data_rows:
        doc_ref = metadata_store.collection(request_json["Collection"]).document(elem[header[0]])
        doc_ref.set(elem)

    reader = metadata_store.collection(request_json["Collection"]).document(request_json["Document"]).get()
    print("Document Data : ", reader.to_dict())



    return "Data Loaded"