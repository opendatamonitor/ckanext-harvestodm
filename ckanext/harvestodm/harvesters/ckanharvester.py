import urllib2
from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckanext.harvestodm.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
import pymongo
import json
import urllib2
import requests
import logging
log = logging.getLogger(__name__)
import datetime
from base import HarvesterBase
import configparser

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
log_path=config['ckan:odm_extensions']['log_path']
ckan_harvester_error_log=str(log_path)+'ckan/Errorlog.txt'
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

client=pymongo.MongoClient(str(mongoclient), int(mongoport))
db=client.odm
odm=db.odm




class CKANHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    config = None

    api_version = 2

    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version


    def _get_content(self, url):
        http_request = urllib2.Request(
            url = url,
        )

        api_key = self.config.get('api_key',None)
        if api_key: 
            http_request.add_header('Authorization',api_key)
        http_response = urllib2.urlopen(http_request)

        return http_response.read()

    def _get_group(self, base_url, group_name):
        url = base_url + self._get_rest_api_offset() + '/group/' + group_name
        try:
            content = self._get_content(url)
            return json.loads(content)
        except Exception, e:
            raise e

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances',
            'form_config_interface':'Text'
        }

    def validate_config(self,config):
        if not config:
            return config
        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('read_only','force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config


    def gather_stage(self,harvest_job):
        log.debug('In CKANHarvester gather_stage (%s)' % harvest_job.source.url)
        get_all_packages = True
        package_ids = []
        config=json.loads(harvest_job.source.config)
        apiversion=config['api_version']

        self._set_config(harvest_job.source.config)

        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source==harvest_job.source) \
                        .filter(HarvestJob.gather_finished!=None) \
                        .filter(HarvestJob.id!=harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        # Get source URL
        base_url = harvest_job.source.url.rstrip('/')
        base_rest_url = base_url + self._get_rest_api_offset()
        base_search_url = base_url + self._get_search_api_offset()

        ##load existing datasets names and ids from mongoDb
        datasets=list(odm.find({'catalogue_url':harvest_job.source.url}))
        datasets_ids=[]
        datasets_names=[]
        j=0
        while j<len(datasets):
		  try:
			datasets_ids.append(datasets[j]['id'])
		  except:pass
		  try:
			datasets_names.append(datasets[j]['name'])
		  except:pass
		  j+=1

        if (previous_job and not previous_job.gather_errors and not len(previous_job.objects) == 0):
            #print('previous job found')
            if not self.config.get('force_all',False):
                get_all_packages = True

                # Request only the packages modified since last harvest job
                #last_time = previous_job.gather_finished.isoformat()
                #url = base_search_url + '/revision?since_time=%s' % last_time

                #try:
                 #   content = self._get_content(url)

                  #  revision_ids = json.loads(content)
                   # if len(revision_ids):
                    #    for revision_id in revision_ids:
                     #       url = base_rest_url + '/revision/%s' % revision_id
                      #      try:
                       #         content = self._get_content(url)
                        #    except Exception,e:
                         #       self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                          #      continue

                           # revision = json.loads(content)
                            #for package_id in revision['packages']:
                             #   if not package_id in package_ids:
                              #      package_ids.append(package_id)
                    #else:
                     #   log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
                      #  return None

                #except urllib2.HTTPError,e:
                 #   if e.getcode() == 400:
                  #      log.info('CKAN instance %s does not suport revision filtering' % base_url)
                   #     get_all_packages = True
                    #else:
                     #   self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                      #  return None



        if get_all_packages:
            # Request all remote packages
            url = base_rest_url + '/package'
            #api 3 -> get action case
            if apiversion==3:
			  url=harvest_job.source.url.rstrip('/')+"/api/3/action/package_list"
			 # print(url)
			  try:
				content = self._get_content(url)
			  except Exception,e:
				self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
				return None
			  content = json.loads(content)
			  package_ids=content['result']
	    if 'http://data.noe.gv.at' in url:
		url='http://data.noe.gv.at/api/search'
	    if 'data.gouv.fr' in url:
		  url="http://qa.data.gouv.fr/api/1/datasets?organization="
            try:
                content = self._get_content(url)
            except Exception,e:
                self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                return None
            if apiversion!=3:
			  package_ids = json.loads(content)
            if 'data.gouv.fr' in url:
		package_ids=package_ids['value']

        ##check for deleted datasets that exist in mongo
        count_pkg_ids=0
        while count_pkg_ids<len(package_ids):
		  temp_pckg_id=package_ids[count_pkg_ids]
		  if temp_pckg_id in datasets_ids:
			datasets_ids.remove(temp_pckg_id)
		  if temp_pckg_id in datasets_names:
			datasets_names.remove(temp_pckg_id)
		  count_pkg_ids+=1
        if len(datasets_names)<len(datasets_ids):
		  #print(datasets_names)
		  j=0
		  #print(base_url)
		  while j<len(datasets_names):
			i=0
			while i<len(datasets):
			  if datasets_names[j] in datasets[i]['name']:
				document=datasets[i]
				document.update({"deleted_dataset":True})
				odm.save(document)
			  i+=1
			#document=odm.find_one({"catalogue_url":str(base_url),"name":datasets_names[j]})
			#if document==None:
			  #document=odm.find_one({"catalogue_url":str(base_url)+"/","name":datasets_names[j]})
			  #print(document)
			#document.update({"deleted_dataset":True})
			#odm.save(document)
			j+=1
        else:
		  #print(datasets_ids)
		  j=0
		  while j<len(datasets_ids):
			i=0
			while i<len(datasets):
			  if datasets_ids[j] in datasets[i]['id']:
				document=datasets[i]
				document.update({"deleted_dataset":True})
				odm.save(document)
			  i+=1
			#document=odm.find_one({"catalogue_url":str(base_url),"id":datasets_ids[j]})
			#if document==None:
			  #document=odm.find_one({"catalogue_url":str(base_url)+"/","id":datasets_ids[j]})
			#document.update({"deleted_dataset":True})
			#odm.save(document)
			j+=1





        try:
            object_ids = []
            if len(package_ids):
                for package_id in package_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = package_id, job = harvest_job)
                    obj.save()
                    object_ids.append(obj.id)

                return object_ids

            else:
               self._save_gather_error('No packages received for URL: %s' % url,
                       harvest_job)
               return None
        except Exception, e:
            self._save_gather_error('%r'%e.message,harvest_job)


    def fetch_stage(self,harvest_object):
        log.debug('In CKANHarvester fetch_stage')
        self._set_config(harvest_object.job.source.config)
        config=json.loads(harvest_object.job.source.config)
        apiversion=config['api_version']
        # Get source URL
        url = harvest_object.source.url.rstrip('/')
        ##set type for custom made ckan catalogues
        types=[]
        config=json.loads(harvest_object.job.source.config)
        if 'type' in config.keys():
		  types=config['type']
        types.append('dataset')
        

	text_file = open(str(ckan_harvester_error_log), "a")
	
	#-- Connect to mongoDb:
	


	db1=db.odm
	db_jobs=db.jobs
	db_fetch_temp=db.fetch_temp
        url = url + self._get_rest_api_offset() + '/package/' + harvest_object.guid
	if 'http://data.noe.gv.at' in url:
		url='http://data.noe.gv.at/api/json/'+harvest_object.guid
	if 'data.gouv.fr' in url:
		url='http://qa.data.gouv.fr/api/1/datasets/'+harvest_object.guid
        if apiversion==3:
		url=harvest_object.source.url.rstrip('/')+'/api/3/action/package_show?id='+harvest_object.guid
        #print(url)	
       	
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_object_error('Unable to get content for package: %s: %r' % \
                                        (url, e),harvest_object)
            return None


        
        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
	try:
        	harvest_object.save()
	except:
		pass
       
	#TRANSFORMATIONS TO JSON FOR MongoDB
	try:
		content=json.loads(content)
                if apiversion==3:
	            try:
	                content=content['result'][0]
	            except:
                        content=content['result']
	            #print(content)
	            i=0
	            found=False
	            while i<len(content.keys()):
		        if content.keys()[i]=='url':
		            found=True
		        i+=1
	            if found==False:
	                dataset_url=harvest_object.source.url.rstrip('/')+"/dataset/"+harvest_object.guid
	                content.update({"url":dataset_url})
		        
	            

		if 'data.gouv.fr' in url:
			content=content['value']
		base_url = harvest_object.source.url
		try:
		  doc=db_jobs.find_one({"cat_url":str(base_url)})
		  language=doc['language']
		  content['extras'].update({"language":language})
		except:
		  pass
	
		content.update({"catalogue_url":str(base_url)})
		content.update({"platform":"ckan"})
		metadata_created=datetime.datetime.now()
		content.update({"metadata_created":str(metadata_created)})
		content.update({"metadata_modified":str(metadata_created)})
		content1=str(content)
		content2=content1.replace("null",'""').replace("true","'true'").replace("false","'false'").replace('""""','""')
		content3="content4="+content2

		#-- STORE to Mongodb
		try:
			exec(content3)
			if 'type' in content4.keys() and (content4['type'] in types or content4['type'].lower() in types):
				try:
					for key, value in content4['extras'].iteritems():
						 if '.' in key:
							 temp=key.replace('.','_')
							 content4['extras'][temp]=value
							 del content4['extras'][key]
					l=0
					while l<len(content4['resources']):
						for key, value in content4['resources'][l].iteritems():
							 if '.' in key:
								 temp=key.replace('.','_')
								 content4['resources'][temp]=value
								 del content4['resources'][key]
						l+=1
					document=db1.find_one({"id":content4['id'],"catalogue_url":content4['catalogue_url']})
					if document==None:
					  db1.save(content4)
					  log.info('Metadata stored succesfully to MongoDb.')
					  print('Metadata stored succesfully to MongoDb.')
					  fetch_document=db_fetch_temp.find_one()
					  if fetch_document==None:
						fetch_document={}
						fetch_document.update({"cat_url":base_url})
						fetch_document.update({"new":1})
						fetch_document.update({"updated":0})
						db_fetch_temp.save(fetch_document)
					  else:
						if base_url==fetch_document['cat_url']:
						  new_count=fetch_document['new']
						  new_count+=1
						  fetch_document.update({"new":new_count})
						  db_fetch_temp.save(fetch_document)
						else:
						  last_cat_url=fetch_document['cat_url']
						  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
						  if 'new' in fetch_document.keys():
							new=fetch_document['new']
							if 'new' in doc.keys():
							  last_new=doc['new']
							  doc.update({"last_new":last_new})
							doc.update({"new":new})
							db_jobs.save(doc)
						  if 'updated' in fetch_document.keys():
							updated=fetch_document['updated']
							if 'updated' in doc.keys():
							  last_updated=doc['updated']
							  doc.update({"last_updated":last_updated})
							doc.update({"updated":updated})
							db_jobs.save(doc)
						  fetch_document.update({"cat_url":base_url})
						  fetch_document.update({"new":1})
						  fetch_document.update({"updated":0})
						  db_fetch_temp.save(fetch_document)				  


					else:
						  	
							  met_created=document['metadata_created']
							  if 'copied' in document.keys():
								content4.update({'copied':document['copied']})
							  content4.update({'metadata_created':met_created})
							  content4.update({'metadata_updated':str(datetime.datetime.now())})
							  content4.update({'updated_dataset':True})
							  objectid=document['_id']
							  content4.update({'_id':objectid})
							  db1.save(content4)
							  log.info('Metadata updated succesfully to MongoDb.')
							  print('Metadata updated succesfully to MongoDb.')
							  fetch_document=db_fetch_temp.find_one()
							  if fetch_document==None:
								fetch_document={}
								fetch_document.update({"cat_url":base_url})
								fetch_document.update({"updated":1})
								fetch_document.update({"new":0})
								db_fetch_temp.save(fetch_document)
							  else:
								if base_url==fetch_document['cat_url']:
								  updated_count=fetch_document['updated']
								  updated_count+=1
								  fetch_document.update({"updated":updated_count})
								  db_fetch_temp.save(fetch_document)
								else:
								  last_cat_url=fetch_document['cat_url']
								  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
								  if 'new' in fetch_document.keys():
									new=fetch_document['new']
									if 'new' in doc.keys():
									  last_new=doc['new']
									  doc.update({"last_new":last_new})
									doc.update({"new":new})
									db_jobs.save(doc)
								  if 'updated' in fetch_document.keys():
									updated=fetch_document['updated']
									if 'updated' in doc.keys():
									  last_updated=doc['updated']
									  doc.update({"last_updated":last_updated})
									doc.update({"updated":updated})
									db_jobs.save(doc)
								  fetch_document.update({"cat_url":base_url})
								  fetch_document.update({"updated":1})
								  fetch_document.update({"new":0})
								  db_fetch_temp.save(fetch_document)

	
				except :
					try:
						document=db1.find_one({"id":content4['id'],"catalogue_url":content4['catalogue_url']})
						if document==None:
						  db1.save(content4)
						  log.info('Metadata stored succesfully to MongoDb.')
						  print('Metadata stored succesfully to MongoDb.')
						  fetch_document=db_fetch_temp.find_one()
						  if fetch_document==None:
							fetch_document={}
							fetch_document.update({"cat_url":base_url})
							fetch_document.update({"new":1})
							fetch_document.update({"updated":0})
							db_fetch_temp.save(fetch_document)
						  else:
							if base_url==fetch_document['cat_url']:
							  new_count=fetch_document['new']
							  new_count+=1
							  fetch_document.update({"new":new_count})
							  db_fetch_temp.save(fetch_document)
							else:
							  last_cat_url=fetch_document['cat_url']
							  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
							  if 'new' in fetch_document.keys():
								new=fetch_document['new']
								if 'new' in doc.keys():
								  last_new=doc['new']
								  doc.update({"last_new":last_new})
								doc.update({"new":new})
								db_jobs.save(doc)
							  if 'updated' in fetch_document.keys():
								updated=fetch_document['updated']
								if 'updated' in doc.keys():
								  last_updated=doc['updated']
								  doc.update({"last_updated":last_updated})
								doc.update({"updated":updated})
								db_jobs.save(doc)
							  fetch_document.update({"cat_url":base_url})
							  fetch_document.update({"new":1})
							  fetch_document.update({"updated":0})
							  db_fetch_temp.save(fetch_document)



						else:
							  met_created=document['metadata_created']
							  if 'copied' in document.keys():
								content4.update({'copied':document['copied']})
							  content4.update({'metadata_created':met_created})
							  content4.update({'metadata_updated':str(datetime.datetime.now())})
							  content4.update({'updated_dataset':True})
							  objectid=document['_id']
							  content4.update({"_id":objectid})
							  db1.save(content4)
							  log.info('Metadata updated succesfully to MongoDb.')
							  print('Metadata updated succesfully to MongoDb.')
							  fetch_document=db_fetch_temp.find_one()
							  if fetch_document==None:
								fetch_document={}
								fetch_document.update({"cat_url":base_url})
								fetch_document.update({"updated":1})
								fetch_document.update({"new":0})
								db_fetch_temp.save(fetch_document)
							  else:
								if base_url==fetch_document['cat_url']:
								  updated_count=fetch_document['updated']
								  updated_count+=1
								  fetch_document.update({"updated":updated_count})
								  db_fetch_temp.save(fetch_document)
								else:
								  last_cat_url=fetch_document['cat_url']
								  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
								  if 'new' in fetch_document.keys():
									new=fetch_document['new']
									if 'new' in doc.keys():
									  last_new=doc['new']
									  doc.update({"last_new":last_new})
									doc.update({"new":new})
									db_jobs.save(doc)
								  if 'updated' in fetch_document.keys():
									updated=fetch_document['updated']
									if 'updated' in doc.keys():
									  last_updated=doc['updated']
									  doc.update({"last_updated":last_updated})
									doc.update({"updated":updated})
									db_jobs.save(doc)
								  fetch_document.update({"cat_url":base_url})
								  fetch_document.update({"updated":1})
								  fetch_document.update({"new":0})
								  db_fetch_temp.save(fetch_document)

					except:
						pass
		except SyntaxError:
			content5="content6="+content1.replace("null",'""').replace('""""','""')
			try:
				exec(content5)
				#text_file.write("Syntax Error")
				if 'type' in content6.keys() and (content6['type'] in types or content6['type'].lower() in types):
					document=db1.find_one({"id":content6['id'],"catalogue_url":content6['catalogue_url']})
					if document==None:
					  db1.save(content6)
					  log.info('Metadata stored succesfully to MongoDb.')
					  print('Metadata stored succesfully to MongoDb.')
					  fetch_document=db_fetch_temp.find_one()
					  if fetch_document==None:
						fetch_document={}
						fetch_document.update({"cat_url":base_url})
						fetch_document.update({"new":1})
						fetch_document.update({"updated":0})
						db_fetch_temp.save(fetch_document)
					  else:
						if base_url==fetch_document['cat_url']:
						  new_count=fetch_document['new']
						  new_count+=1
						  fetch_document.update({"new":new_count})
						  db_fetch_temp.save(fetch_document)
						else:
						  last_cat_url=fetch_document['cat_url']
						  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
						  if 'new' in fetch_document.keys():
							new=fetch_document['new']
							if 'new' in doc.keys():
							  last_new=doc['new']
							  doc.update({"last_new":last_new})
							doc.update({"new":new})
							db_jobs.save(doc)
						  if 'updated' in fetch_document.keys():
							updated=fetch_document['updated']
							if 'updated' in doc.keys():
							  last_updated=doc['updated']
							  doc.update({"last_updated":last_updated})
							doc.update({"updated":updated})
							db_jobs.save(doc)
						  fetch_document.update({"cat_url":base_url})
						  fetch_document.update({"new":1})
						  fetch_document.update({"updated":0})
						  db_fetch_temp.save(fetch_document)			


					else:

							  met_created=document['metadata_created']
							  if 'copied' in document.keys():
								content6.update({'copied':document['copied']})
							  content6.update({'metadata_created':met_created})
							  content6.update({'metadata_updated':str(datetime.datetime.now())})
							  content6.update({'updated_dataset':True})
							  objectid=document['_id']
							  content6.update({"_id":objectid})
							  db1.save(content6)
							  log.info('Metadata updated succesfully to MongoDb.')
							  print('Metadata updated succesfully to MongoDb.')
							  fetch_document=db_fetch_temp.find_one()
							  #print("fetcher:"+str(fetch_document))
							  if fetch_document==None:
								
								fetch_document={}
								fetch_document.update({"cat_url":base_url})
								fetch_document.update({"updated":1})
								fetch_document.update({"new":0})
								db_fetch_temp.save(fetch_document)
							  else:
								if base_url==fetch_document['cat_url']:
								  updated_count=fetch_document['updated']
								  updated_count+=1
								  fetch_document.update({"updated":updated_count})
								  db_fetch_temp.save(fetch_document)
								else:
								  last_cat_url=fetch_document['cat_url']
								  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
								  if 'new' in fetch_document.keys():
									new=fetch_document['new']
									if 'new' in doc.keys():
									  last_new=doc['new']
									  doc.update({"last_new":last_new})
									doc.update({"new":new})
									db_jobs.save(doc)
								  if 'updated' in fetch_document.keys():
									updated=fetch_document['updated']
									if 'updated' in doc.keys():
									  last_updated=doc['updated']
									  doc.update({"last_updated":last_updated})
									doc.update({"updated":updated})
									db_jobs.save(doc)
								  fetch_document.update({"cat_url":base_url})
								  fetch_document.update({"updated":1})
								  fetch_document.update({"new":0})
								  db_fetch_temp.save(fetch_document)

			except SyntaxError:
				try:
					content5="content6="+content1.replace("null",'""').replace('""""','""').replace('1.0','"1.0"').replace('2.0','"2.0"').replace('3.0','"3.0"').replace('4.0','"4.0"').replace('5.0','"5.0"')
					exec(content5)
		#	text_file.write("Syntax Error")
					if 'type' in content6.keys() and (content6['type'] in types or content6['type'].lower() in types):
						for key, value in content6['extras'].iteritems():
						    if '.' in key:
							 temp=key.replace('.','_')
							 content6['extras'][temp]=value
							 del content6['extras'][key]
						l=0
						while l<len(content4['resources']):
							for key, value in content6['resources'][l].iteritems():
								 if '.' in key:
									 temp=key.replace('.','_')
									 content6['resources'][temp]=value
									 del content6['resources'][key]
							l+=1
						document=db1.find_one({"id":content6['id'],"catalogue_url":content6['catalogue_url']})
						if document==None:
						  db1.save(content6)
						  log.info('Metadata stored succesfully to MongoDb.')
						  print('Metadata stored succesfully to MongoDb.')
						  fetch_document=db_fetch_temp.find_one()
						  if fetch_document==None:
							fetch_document={}
							fetch_document.update({"cat_url":base_url})
							fetch_document.update({"new":1})
							fetch_document.update({"updated":0})
							db_fetch_temp.save(fetch_document)
						  else:
							if base_url==fetch_document['cat_url']:
							  new_count=fetch_document['new']
							  new_count+=1
							  fetch_document.update({"new":new_count})
							  db_fetch_temp.save(fetch_document)
							else:
							  last_cat_url=fetch_document['cat_url']
							  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
							  if 'new' in fetch_document.keys():
								new=fetch_document['new']
								if 'new' in doc.keys():
								  last_new=doc['new']
								  doc.update({"last_new":last_new})
								doc.update({"new":new})
								db_jobs.save(doc)
							  if 'updated' in fetch_document.keys():
								updated=fetch_document['updated']
								if 'updated' in doc.keys():
								  last_updated=doc['updated']
								  doc.update({"last_updated":last_updated})
								doc.update({"updated":updated})
								db_jobs.save(doc)
							  fetch_document.update({"cat_url":base_url})
							  fetch_document.update({"new":1})
							  fetch_document.update({"updated":0})
							  db_fetch_temp.save(fetch_document)


						else:

							  met_created=document['metadata_created']
							  if 'copied' in document.keys():
								content6.update({'copied':document['copied']})
							  content6.update({'metadata_created':met_created})
							  content6.update({'metadata_updated':str(datetime.datetime.now())})
							  content6.update({'updated_dataset':True})
							  objectid=document['_id']
							  content6.update({"_id":objectid})
							  db1.save(content6)
							  log.info('Metadata updated succesfully to MongoDb.')
							  print('Metadata updated succesfully to MongoDb.')
							  fetch_document=db_fetch_temp.find_one()
							  if fetch_document==None:
								fetch_document={}
								fetch_document.update({"cat_url":base_url})
								fetch_document.update({"updated":1})
								fetch_document.update({"new":0})
								db_fetch_temp.save(fetch_document)
							  else:
								if base_url==fetch_document['cat_url']:
								  updated_count=fetch_document['updated']
								  updated_count+=1
								  fetch_document.update({"updated":updated_count})
								  db_fetch_temp.save(fetch_document)
								else:
								  last_cat_url=fetch_document['cat_url']
								  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
								  if 'new' in fetch_document.keys():
									new=fetch_document['new']
									if 'new' in doc.keys():
									  last_new=doc['new']
									  doc.update({"last_new":last_new})
									doc.update({"new":new})
									db_jobs.save(doc)
								  if 'updated' in fetch_document.keys():
									updated=fetch_document['updated']
									if 'updated' in doc.keys():
									  last_updated=doc['updated']
									  doc.update({"last_updated":last_updated})
									doc.update({"updated":updated})
									db_jobs.save(doc)
								  fetch_document.update({"cat_url":base_url})
								  fetch_document.update({"updated":1})
								  fetch_document.update({"new":0})
								  db_fetch_temp.save(fetch_document)
				except SyntaxError:
					text_file.write('Json Error : '+'\n')
					text_file.write(str(content)+'\n'+'\n')

	except:pass
        
        return True

    def import_stage(self,harvest_object):
        log.debug('In CKANHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)
            if 'data.gouv.fr' in  package_dict['url']:
            	package_dict=package_dict['value']
		


            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags',[])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []
                context = {'model': model, 'session': Session, 'user': 'harvest'}

                for group_name in package_dict['groups']:
                    try:
                        data_dict = {'id': group_name}
                        group = get_action('group_show')(context, data_dict)
                        if self.api_version == 1:
                            validated_groups.append(group['name'])
                        else:
                            validated_groups.append(group['id'])
                    except NotFound, e:
                        log.info('Group %s is not available' % group_name)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_name)
                            except:
                                log.error('Could not get remote group %s' % group_name)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)
                            get_action('group_create')(context, group)
                            log.info('Group %s has been newly created' % group_name)
                            if self.api_version == 1:
                                validated_groups.append(group['name'])
                            else:
                                validated_groups.append(group['id'])

                package_dict['groups'] = validated_groups

            context = {'model': model, 'session': Session, 'user': 'harvest'}

            # Local harvest source organization
            source_dataset = get_action('package_show')(context, {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if not remote_orgs in ('only_local', 'create'):
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                if not 'owner_org' in package_dict:
                    package_dict['owner_org'] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict['owner_org']

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(context, data_dict)
                        validated_org = org['id']
                    except NotFound, e:
                        log.info('Organization %s is not available' % remote_org)
                        if remote_orgs == 'create':
                            try:
                                org = self._get_group(harvest_object.source.url, remote_org)
                                for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                    org.pop(key, None)
                                get_action('organization_create')(context, org)
                                log.info('Organization %s has been newly created' % remote_org)
                                validated_org = org['id']
                            except:
                                log.error('Could not get remote org %s' % remote_org)

                package_dict['owner_org'] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])

            # Find any extras whose values are not strings and try to convert
            # them to strings, as non-string extras are not allowed anymore in
            # CKAN 2.0.
            for key in package_dict['extras'].keys():
                if not isinstance(package_dict['extras'][key], basestring):
                    try:
                        package_dict['extras'][key] = json.dumps(
                                package_dict['extras'][key])
                    except TypeError:
                        # If converting to a string fails, just delete it.
                        del package_dict['extras'][key]

            # Set default extras if needed
            default_extras = self.config.get('default_extras',{})
            if default_extras:
                override_extras = self.config.get('override_extras',False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key,value in default_extras.iteritems():
                    if not key in package_dict['extras'] or override_extras:
                        # Look for replacement strings
                        if isinstance(value,basestring):
                            value = value.format(harvest_source_id=harvest_object.job.source.id,
                                     harvest_source_url=harvest_object.job.source.url.strip('/'),
                                     harvest_source_title=harvest_object.job.source.title,
                                     harvest_job_id=harvest_object.job.id,
                                     harvest_object_id=harvest_object.id,
                                     dataset_id=package_dict['id'])

                        package_dict['extras'][key] = value

            # Clear remote url_type for resources (eg datastore, upload) as we
            # are only creating normal resources with links to the remote ones
            for resource in package_dict.get('resources', []):
                resource.pop('url_type', None)

            result = self._create_or_update_package(package_dict,harvest_object)

            if result and self.config.get('read_only',False) == True:

                package = model.Package.get(package_dict['id'])

                # Clear default permissions
                model.clear_user_roles(package)

                # Setup harvest user as admin
                user_name = self.config.get('user',u'harvest')
                user = model.User.get(user_name)
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

                # Other users can only read
                for user_name in (u'visitor',u'logged_in'):
                    user = model.User.get(user_name)
                    pkg_role = model.PackageRole(package=package, user=user, role=model.Role.READER)


            return True
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')


