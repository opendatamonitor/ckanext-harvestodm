import types
from logging import getLogger

from sqlalchemy.util import OrderedDict

from ckan import logic
from ckan import model
import ckan.plugins as p
from ckan.lib.plugins import DefaultDatasetForm
from ckan.lib.navl import dictization_functions

from ckanext.harvestodm import logic as harvest_logic
from ckan.lib.base import c
from ckanext.harvestodm.model import setup as model_setup
from ckanext.harvestodm.model import HarvestSource, HarvestJob, HarvestObject
import pymongo
##
import ckan.plugins.toolkit as tk
##
import configparser
import datetime

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']
client = pymongo.MongoClient(str(mongoclient), int(mongoport))

log = getLogger(__name__)
assert not log.disabled

DATASET_TYPE_NAME = 'harvest'

class Harvest(p.SingletonPlugin, tk.DefaultDatasetForm):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IDatasetForm)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IFacets, inherit=True)

    startup = False

    ## IPackageController

    def after_create(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME and not self.startup:
            # Create an actual HarvestSource object
            _create_harvest_source_object(context, data_dict)

    def after_update(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # Edit the actual HarvestSource object
            _update_harvest_source_object(context, data_dict)

    def after_delete(self, context, data_dict):

        package_dict = p.toolkit.get_action('package_show')(context, {'id': data_dict['id']})
    # delete job from mongo collections : catalogregistry , html_jobs , jobs 
        db = client.odm
        
        try:
	        collection=db.jobs
        except AttributeError as e:
	        log.warn('error: {0}', e)
	        
        try:
	        collection1=db.html_jobs
        except AttributeError as e:
	        log.warn('error: {0}', e)
	    
        try:
	        collection2=db.catalogregistry
        except AttributeError as e:
	        log.warn('error: {0}', e)
	    
        try:
		  document=collection.remove({"base_url":package_dict['url']})
        except:pass
        document1=collection1.remove({"cat_url":package_dict['url']})
        document2=collection2.remove({"cat_url":package_dict['url']})

        
        if 'type' in package_dict and package_dict['type'] == DATASET_TYPE_NAME:
            # Delete the actual HarvestSource object
            _delete_harvest_source_object(context, package_dict)

    def before_view(self, data_dict):

        # check_ckan_version should be more clever than this
        if p.toolkit.check_ckan_version(max_version='2.1.99') and (
           not 'type' in data_dict or data_dict['type'] != DATASET_TYPE_NAME):
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource
            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id==data_dict['id']) \
                    .filter(HarvestObject.current==True) \
                    .first()

            if harvest_object:
                for key, value in [
                    ('harvest_object_id', harvest_object.id),
                    ('harvest_source_id', harvest_object.source.id),
                    ('harvest_source_title', harvest_object.source.title),
                        ]:
                    _add_extra(data_dict, key, value)
        return data_dict


    def after_show(self, context, data_dict):

        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict['id'])
            if not source:
                log.error('Harvest source not found for dataset {0}'.format(data_dict['id']))
                return data_dict

            data_dict['status'] = p.toolkit.get_action('harvest_source_show_status')(context, {'id': source.id})

        elif not 'type' in data_dict or data_dict['type'] != DATASET_TYPE_NAME:
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource

            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id==data_dict['id']) \
                    .filter(HarvestObject.current==True) \
                    .first()

            # If the harvest extras are there, remove them. This can happen eg
            # when calling package_update or resource_update, which call
            # package_show
            if data_dict.get('extras'):
                data_dict['extras'][:] = [e for e in data_dict.get('extras', [])
                                          if not e['key']
                                          in ('harvest_object_id', 'harvest_source_id', 'harvest_source_title',)]


            # We only want to add these extras at index time so they are part
            # of the cached data_dict used to display, search results etc. We
            # don't want them added when editing the dataset, otherwise we get
            # duplicated key errors.
            # The only way to detect indexing right now is checking that
            # validate is set to False.
            if harvest_object and not context.get('validate', True):
                for key, value in [
                    ('harvest_object_id', harvest_object.id),
                    ('harvest_source_id', harvest_object.source.id),
                    ('harvest_source_title', harvest_object.source.title),
                        ]:
                    _add_extra(data_dict, key, value)

        return data_dict

    ## IDatasetForm

    def is_fallback(self):
        return False

    def package_types(self):
        return [DATASET_TYPE_NAME]

    def package_form(self):
        return 'source/new_source_form.html'

    def search_template(self):
        return 'source/search.html'

    def read_template(self):
        return 'source/read.html'

    def new_template(self):
        return 'source/new.html'

    def edit_template(self):
        return 'source/edit.html'

    def setup_template_variables(self, context, data_dict):

        p.toolkit.c.harvest_source = p.toolkit.c.pkg_dict

        p.toolkit.c.dataset_type = DATASET_TYPE_NAME


    def create_package_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvestodm.logic.schema import harvest_source_create_package_schema
        schema = harvest_source_create_package_schema()
        if self.startup:
            schema['id'] = [unicode]
	schema.update({
            'catalogue_country': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'catalogue_date_created': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
			
	schema.update({
            'metadata_mappings': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'catalogue_date_updated': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'language': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
			
	schema.update({
            'datasets_list_url': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'dataset_url': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'datasets_list_identifier': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
	schema.update({
            'dataset_id': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
        return schema

	schema.update({
            'apikey': [tk.get_validator('ignore_missing'),
                            tk.get_converter('convert_to_extras')]
        })
        return schema

    def update_package_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvestodm.logic.schema import harvest_source_update_package_schema
        schema = harvest_source_update_package_schema()

        return schema

    def show_package_schema(self):
        '''
        Returns the schema for mapping package data from the database into a
        format suitable for the form
        '''
        from ckanext.harvestodm.logic.schema import harvest_source_show_package_schema

        return harvest_source_show_package_schema()

    def configure(self, config):

        self.startup = True

        # Setup harvest model
        model_setup()

        self.startup = False

    def before_map(self, map):

        # Most of the routes are defined via the IDatasetForm interface
        # (ie they are the ones for a package type)
        controller = 'ckanext.harvestodm.controllers.view:ViewController'

        map.connect('{0}_delete'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/delete/:id',controller=controller, action='delete')
        map.connect('{0}_refresh'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/refresh/:id',controller=controller,
                action='refresh')
        map.connect('{0}_admin'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/admin/:id', controller=controller, action='admin')
        map.connect('{0}_about'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/about/:id', controller=controller, action='about')
        map.connect('{0}_clear'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/clear/:id', controller=controller, action='clear')

        map.connect('harvest_job_list', '/' + DATASET_TYPE_NAME + '/{source}/job', controller=controller, action='list_jobs')
        map.connect('harvest_job_show_last', '/' + DATASET_TYPE_NAME + '/{source}/job/last', controller=controller, action='show_last_job')
        map.connect('harvest_job_show', '/' + DATASET_TYPE_NAME + '/{source}/job/{id}', controller=controller, action='show_job')

        map.connect('harvest_object_show', '/' + DATASET_TYPE_NAME + '/object/:id', controller=controller, action='show_object')
        map.connect('harvest_object_for_dataset_show', '/dataset/harvest_object/:id', controller=controller, action='show_object', ref_type='dataset')

        org_controller = 'ckanext.harvestodm.controllers.organization:OrganizationController'
        map.connect('{0}_org_list'.format(DATASET_TYPE_NAME), '/organization/' + DATASET_TYPE_NAME + '/' + '{id}', controller=org_controller, action='source_list')

        return map

    def update_config(self, config):
        # check if new templates
        templates = 'templates'
        if p.toolkit.check_ckan_version(min_version='2.0'):
            if not p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
                templates = 'templates_new'
        p.toolkit.add_template_directory(config, templates)
        p.toolkit.add_public_directory(config, 'public')
        p.toolkit.add_resource('fanstatic_library', 'ckanext-harvest')
        p.toolkit.add_resource('public/ckanext/harvestodm/javascript', 'harvest-extra-field')

    ## IActions

    def get_actions(self):

        module_root = 'ckanext.harvestodm.logic.action'
        action_functions = _get_logic_functions(module_root)

        return action_functions

    ## IAuthFunctions

    def get_auth_functions(self):

        module_root = 'ckanext.harvestodm.logic.auth'
        auth_functions = _get_logic_functions(module_root)

        return auth_functions

    ## ITemplateHelpers

    def get_helpers(self):
        from ckanext.harvestodm import helpers as harvest_helpers
        return {
                'package_list_for_source': harvest_helpers.package_list_for_source,
                'harvesters_info': harvest_helpers.harvesters_info,
                'harvester_types': harvest_helpers.harvester_types,
                'harvest_frequencies': harvest_helpers.harvest_frequencies,
                'link_for_harvest_object': harvest_helpers.link_for_harvest_object,
                'countries_list': harvest_helpers.countries_list,
                'languages_list': harvest_helpers.languages_list,
                'harvest_source_extra_fields': harvest_helpers.harvest_source_extra_fields,
                }

    def dataset_facets(self, facets_dict, package_type):

        if package_type <> 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type','Type'),
                           ])

    def organization_facets(self, facets_dict, organization_type, package_type):

        if package_type <> 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type','Type'),
                           ])

def _add_extra(data_dict, key, value):
    if not 'extras' in data_dict:
        data_dict['extras'] = []

    data_dict['extras'].append({
        'key': key, 'value': value, 'state': u'active'
    })

def _get_logic_functions(module_root, logic_functions = {}):

    for module_name in ['get', 'create', 'update','delete']:
        module_path = '%s.%s' % (module_root, module_name,)
        try:
            module = __import__(module_path)
        except ImportError:
            log.debug('No auth module for action "{0}"'.format(module_name))
            continue

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_') and  (hasattr(value, '__call__')
                        and (value.__module__ == module_path)):
                logic_functions[key] = value

    return logic_functions

def _create_harvest_source_object(context, data_dict):
    '''
        Creates an actual HarvestSource object with the data dict
        of the harvest_source dataset. All validation and authorization
        checks should be used by now, so this function is not to be used
        directly to create harvest sources. The created harvest source will
        have the same id as the dataset.

        :param data_dict: A standard package data_dict

        :returns: The created HarvestSource object
        :rtype: HarvestSource object
    '''

    log.info('Creating harvest source: %r', data_dict)
    print('##############################')
    print(context)
    print(data_dict)

    source = HarvestSource()
    language_mappings={'English':'en','Bulgarian':'bg','Croatian':'hr','Czech':'cs',\
'Danish':'da','German':'de','Greek':'el','Spanish':'es','Estonian':'et','Finnish':'fi',\
'French':'fr','Hungarian':'hu','Italian':'it','Lithuanian':'lt','Latvian':'lv','Icelandic':'is',\
'Maltese':'mt','Dutch':'nl','Polish':'pl','Portuguese':'pt','Romanian':'ro','Slovak':'sk','Swedish':'sv','Ukrainian':'uk','Norwegian':'no'}
    source.id = data_dict['id']
    source.url = data_dict['url'].strip()
    source.catalogue_country=data_dict['catalogue_country']
    if data_dict['language'] in language_mappings.keys():
	  lang_mapping=language_mappings[str(data_dict['language'])]
	  source.language=lang_mapping
    else:
	  source.language=str(data_dict['language'])
    source.catalogue_date_created=data_dict['catalogue_date_created']
    source.catalogue_date_updated=data_dict['catalogue_date_updated']
    # Avoids clashes with the dataset type
    source.type = data_dict['source_type']
    source.description=data_dict['notes']
    if source.type=='html':
	  if 'http' in source.url and 'https' not in source.url :
			  base_url1=source.url[7:]
			  if '/' in base_url1:
				base_url1=base_url1[:base_url1.find('/')]
			  base_url='http://'+str(base_url1)

	  if 'https' in source.url:
			  base_url1=source.url[8:]
			  if '/' in base_url1:
				base_url1=base_url1[:base_url1.find('/')]
			  base_url='https://'+str(base_url1)
    else: base_url=source.url

    #source.country=data['country']	
    opt = ['active', 'title', 'description', 'user_id',
           'publisher_id', 'config', 'frequency']
    for o in opt:
        if o in data_dict and data_dict[o] is not None:
            source.__setattr__(o,data_dict[o])

    source.active = not data_dict.get('state', None) == 'deleted'

    # Don't commit yet, let package_create do it
    source.add()
    log.info('Harvest source created: %s', source.id)

    ##---------------save job to mongodb--------
    client=pymongo.MongoClient(str(mongoclient),int(mongoport))
    job={"cat_url":str(base_url),"base_url":str(source.url),"type":str(source.type),"id":str(source.id),"description":str(source.description),"frequency":str(source.frequency),
		 "title":str(source.title),'country':str(source.catalogue_country),'language':str(source.language),'catalogue_date_created':str(source.catalogue_date_created),
		 'catalogue_date_updated':str(source.catalogue_date_updated),'date_harvested':datetime.datetime.now(),'user':str(c.user)}
    if 'metadata_mappings' in data_dict.keys():
	  job.update({"metadata_mappings":data_dict["metadata_mappings"]})
    if 'datasets_list_url' in data_dict.keys():
	  job.update({"datasets_list_url":data_dict["datasets_list_url"]})
    if 'dataset_url' in data_dict.keys():
	  job.update({"dataset_url":data_dict["dataset_url"]})
    if 'datasets_list_identifier' in data_dict.keys():
	  job.update({"datasets_list_identifier":data_dict["datasets_list_identifier"]})
    if 'dataset_id' in data_dict.keys():
	  job.update({"dataset_id":data_dict["dataset_id"]})
    if 'apikey' in data_dict['__extras'].keys():
	  job.update({"apikey":data_dict['__extras']["apikey"]})
    db=client.odm
    collection=db.jobs
    collection.save(job)



    return source

def _update_harvest_source_object(context, data_dict):
    '''
        Updates an actual HarvestSource object with the data dict
        of the harvest_source dataset. All validation and authorization
        checks should be used by now, so this function is not to be used
        directly to update harvest sources.

        :param data_dict: A standard package data_dict

        :returns: The created HarvestSource object
        :rtype: HarvestSource object
    '''
    language_mappings={'English':'en','Bulgarian':'bg','Croatian':'hr','Czech':'cs','Danish':'da','Icelandic':'is','German':'de','Greek':'el','Spanish':'es','Estonian':'et','Finnish':'fi','French':'fr','Hungarian':'hu','Italian':'it','Lithuanian':'lt','Latvian':'lv','Maltese':'mt','Dutch':'nl','Polish':'pl','Portuguese':'pt','Romanian':'ro','Slovak':'sk','Swedish':'sv','Ukrainian':'uk','Norwegian':'no'}
    source_id = data_dict.get('id')
    log.info('Harvest source %s update: %r', source_id, data_dict)
    source = HarvestSource.get(source_id)
    if not source:
        log.error('Harvest source %s does not exist', source_id)
        raise logic.NotFound('Harvest source %s does not exist' % source_id)


    fields = ['url', 'title', 'description', 'user_id',
              'publisher_id', 'frequency']
    for f in fields:
        if f in data_dict and data_dict[f] is not None:
            if f == 'url':
                data_dict[f] = data_dict[f].strip()
            source.__setattr__(f,data_dict[f])

    # Avoids clashes with the dataset type
    if 'source_type' in data_dict:
        source.type = data_dict['source_type']

    if 'config' in data_dict:
        source.config = data_dict['config']

    # Don't change state unless explicitly set in the dict
    if 'state' in data_dict:
      source.active = data_dict.get('state') == 'active'

    # Don't commit yet, let package_create do it
    source.add()

    # Abort any pending jobs
    if not source.active:
        jobs = HarvestJob.filter(source=source,status=u'New')
        log.info('Harvest source %s not active, so aborting %i outstanding jobs', source_id, jobs.count())
        if jobs:
            for job in jobs:
                job.status = u'Aborted'
                job.add()

    client=pymongo.MongoClient(str(mongoclient),int(mongoport))
    db=client.odm
    db_jobs=db.jobs
    if source.type=='html':
	  if 'http' in source.url and 'https' not in source.url :
			  base_url1=source.url[7:]
			  if '/' in base_url1:
				base_url1=base_url1[:base_url1.find('/')]
			  base_url='http://'+str(base_url1)

	  if 'https' in source.url:
			  base_url1=source.url[8:]
			  if '/' in base_url1:
				base_url1=base_url1[:base_url1.find('/')]
			  base_url='https://'+str(base_url1)
    else: base_url=source.url
    #try:
    print(base_url)
    job1=db_jobs.find_one({"cat_url":base_url})
    if job1!=None:
       
    #except:
	  #pass
    
       job={"cat_url":str(base_url),"base_url":str(source.url),"type":str(source.type),"id":str(source.id),"description":str(job1['description']),"frequency":str(source.frequency),
		 "title":str(source.title),'country':str(data_dict['__extras']['catalogue_country']),'language':language_mappings[str(data_dict['__extras']['language'])],'catalogue_date_created':str(data_dict['__extras']['catalogue_date_created']),
		 'catalogue_date_updated':str(data_dict['__extras']['catalogue_date_updated']),'user':str(job1['user'])}
       if 'harmonisation' in job1.keys():
          job.update({'harmonisation':job1['harmonisation']})
       if 'official' in job1.keys():
          job.update({'official':job1['official']})
       if 'date_harvested' in job1.keys():
          job.update({'date_harvested':job1['date_harvested']})
       else:
          job.update({'date_harvested':datetime.datetime.now()})
       db_jobs.remove({'id':job1['id']})
       db_jobs.save(job)
       

    return source

def _delete_harvest_source_object(context, data_dict):
    '''
        Deletes an actual HarvestSource object with the id provided on the
        data dict of the harvest_source dataset. Similarly to the datasets,
        the source object is not actually deleted, just flagged as inactive.
        All validation and authorization checks should be used by now, so
        this function is not to be used directly to delete harvest sources.

        :param data_dict: A standard package data_dict

        :returns: The deleted HarvestSource object
        :rtype: HarvestSource object
    '''

    source_id = data_dict.get('id')

    log.info('Deleting harvest source: %s', source_id)
    db = client.odm
    collection=db.jobs
    document=collection.remove({"base_url":data_dict['url']})

    source = HarvestSource.get(source_id)
    if not source:
        log.warn('Harvest source %s does not exist', source_id)
        raise p.toolkit.ObjectNotFound('Harvest source %s does not exist' % source_id)

    # Don't actually delete the record, just flag it as inactive
    source.active = False
    source.save()

    # Abort any pending jobs
    jobs = HarvestJob.filter(source=source, status=u'New')
    if jobs:
        log.info('Aborting %i jobs due to deleted harvest source', jobs.count())
        for job in jobs:
            job.status = u'Aborted'
            job.save()

    log.debug('Harvest source %s deleted', source_id)

    return source
