import hashlib
import json

import logging
import datetime

from pylons import config
from paste.deploy.converters import asbool
from sqlalchemy import and_, or_

from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import PluginImplementations
from ckan.logic import get_action
from ckanext.harvestodm.interfaces import IHarvester
from ckan.lib.search.common import SearchIndexError, make_connection


from ckan.model import Package
from ckan import logic

from ckan.logic import NotFound, check_access

from ckanext.harvestodm.plugin import DATASET_TYPE_NAME
from ckanext.harvestodm.queue import get_gather_publisher, resubmit_jobs

from ckanext.harvestodm.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvestodm.logic import HarvestJobExists
from ckanext.harvestodm.logic.schema import harvest_source_show_package_schema

from ckanext.harvestodm.logic.action.get import harvest_source_show, harvest_job_list, _get_sources_for_user
import json
import pymongo
import bson
import configparser
import logging
##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

log = logging.getLogger(__name__)
client = pymongo.MongoClient(str(mongoclient), int(mongoport))

log = logging.getLogger(__name__)

def harvest_source_update(context,data_dict):
    '''
    Updates an existing harvest source

    This method just proxies the request to package_update,
    which will create a harvest_source dataset type and the
    HarvestSource object. All auth checks and validation will
    be done there .We only make sure to set the dataset type

    Note that the harvest source type (ckan, waf, csw, etc)
    is now set via the source_type field.

    :param id: the name or id of the harvest source to update
    :type id: string
    :param url: the URL for the harvest source
    :type url: string
    :param name: the name of the new harvest source, must be between 2 and 100
        characters long and contain only lowercase alphanumeric characters
    :type name: string
    :param title: the title of the dataset (optional, default: same as
        ``name``)
    :type title: string
    :param notes: a description of the harvest source (optional)
    :type notes: string
    :param source_type: the harvester type for this source. This must be one
        of the registerd harvesters, eg 'ckan', 'csw', etc.
    :type source_type: string
    :param frequency: the frequency in wich this harvester should run. See
        ``ckanext.harvest.model`` source for possible values. Default is
        'MANUAL'
    :type frequency: string
    :param config: extra configuration options for the particular harvester
        type. Should be a serialized as JSON. (optional)
    :type config: string


    :returns: the newly created harvest source
    :rtype: dictionary

    '''
    log.info('Updating harvest source: %r', data_dict)

    data_dict['type'] = DATASET_TYPE_NAME

    context['extras_as_string'] = True
    source = logic.get_action('package_update')(context, data_dict)

    return source

def harvest_source_clear(context,data_dict):
    '''
    Clears all datasets, jobs and objects related to a harvest source, but keeps the source itself.
    This is useful to clean history of long running harvest sources to start again fresh.

    :param id: the id of the harvest source to clear
    :type id: string

    '''
    check_access('harvest_source_clear',context,data_dict)

    harvest_source_id = data_dict.get('id',None)

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    # Clear all datasets from this source from the index
    harvest_source_index_clear(context, data_dict)

    model = context['model']

    sql = "select id from related where id in (select related_id from related_dataset where dataset_id in (select package_id from harvest_object where harvest_source_id = '{harvest_source_id}'));".format(harvest_source_id=harvest_source_id)
    result = model.Session.execute(sql)
    ids = []
    for row in result:
        ids.append(row[0])
    related_ids = "('" + "','".join(ids) + "')"

    sql = '''begin; update package set state = 'to_delete' where id in (select package_id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_error where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_extra where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    delete from harvest_gather_error where harvest_job_id in (select id from harvest_job where source_id = '{harvest_source_id}');
    delete from harvest_job where source_id = '{harvest_source_id}';
    delete from package_role where package_id in (select id from package where state = 'to_delete' );
    delete from user_object_role where id not in (select user_object_role_id from package_role) and context = 'Package';
    delete from resource_revision where resource_group_id in (select id from resource_group where package_id in (select id from package where state = 'to_delete'));
    delete from resource_group_revision where package_id in (select id from package where state = 'to_delete');
    delete from package_tag_revision where package_id in (select id from package where state = 'to_delete');
    delete from member_revision where table_id in (select id from package where state = 'to_delete');
    delete from package_extra_revision where package_id in (select id from package where state = 'to_delete');
    delete from package_revision where id in (select id from package where state = 'to_delete');
    delete from package_tag where package_id in (select id from package where state = 'to_delete');
    delete from resource where resource_group_id in (select id from resource_group where package_id in (select id from package where state = 'to_delete'));
    delete from package_extra where package_id in (select id from package where state = 'to_delete');
    delete from member where table_id in (select id from package where state = 'to_delete');
    delete from resource_group where package_id  in (select id from package where state = 'to_delete');
    delete from related_dataset where dataset_id in (select id from package where state = 'to_delete');
    delete from related where id in {related_ids};
    delete from package where id in (select id from package where state = 'to_delete'); commit;'''.format(harvest_source_id=harvest_source_id, related_ids=related_ids)

    model.Session.execute(sql)

    # Refresh the index for this source to update the status object
    get_action('harvest_source_reindex')(context, {'id': harvest_source_id})

    return {'id': harvest_source_id}

def harvest_source_index_clear(context,data_dict):

    check_access('harvest_source_clear',context,data_dict)
    harvest_source_id = data_dict.get('id',None)

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    conn = make_connection()
    query = ''' +%s:"%s" +site_id:"%s" ''' % ('harvest_source_id', harvest_source_id,
                                            config.get('ckan.site_id'))
    try:
        conn.delete_query(query)
        if asbool(config.get('ckan.search.solr_commit', 'true')):
            conn.commit()
    except Exception, e:
        log.exception(e)
        raise SearchIndexError(e)
    finally:
        conn.close()

    return {'id': harvest_source_id}

def harvest_objects_import(context,data_dict):
    '''
        Reimports the current harvest objects
        It performs the import stage with the last fetched objects, optionally
        belonging to a certain source.
        Please note that no objects will be fetched from the remote server.
        It will only affect the last fetched objects already present in the
        database.
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import',context,data_dict)

    model = context['model']
    session = context['session']
    source_id = data_dict.get('source_id',None)
    harvest_object_id = data_dict.get('harvest_object_id',None)
    package_id_or_name = data_dict.get('package_id',None)

    segments = context.get('segments',None)

    join_datasets = context.get('join_datasets',True)

    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_id)
            raise Exception('This harvest source is not active')

        last_objects_ids = session.query(HarvestObject.id) \
                .join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True)

    elif harvest_object_id:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.id==harvest_object_id)
    elif package_id_or_name:
        last_objects_ids = session.query(HarvestObject.id) \
            .join(Package) \
            .filter(HarvestObject.current==True) \
            .filter(Package.state==u'active') \
            .filter(or_(Package.id==package_id_or_name,
                        Package.name==package_id_or_name))
        join_datasets = False
    else:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.current==True)

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state==u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0

    for obj_id in last_objects_ids:
        if segments and str(hashlib.md5(obj_id[0]).hexdigest())[0] not in segments:
            continue

        obj = session.query(HarvestObject).get(obj_id)

        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester,'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                break
        last_objects_count += 1
    log.info('Harvest objects imported: %s', last_objects_count)
    return last_objects_count

def _caluclate_next_run(frequency):

    now = datetime.datetime.utcnow()
    if frequency == 'ALWAYS':
        return now
    if frequency == 'WEEKLY':
        return now + datetime.timedelta(weeks=1)
    if frequency == 'BIWEEKLY':
        return now + datetime.timedelta(weeks=2)
    if frequency == 'DAILY':
        return now + datetime.timedelta(days=1)
    if frequency == 'MONTHLY':
        if now.month in (4,6,9,11):
            days = 30
        elif now.month == 2:
            if now.year % 4 == 0:
                days = 29
            else:
                days = 28
        else:
            days = 31
        return now + datetime.timedelta(days=days)
    raise Exception('Frequency {freq} not recognised'.format(freq=frequency))


def _make_scheduled_jobs(context, data_dict):

    data_dict = {'only_to_run': True,
                 'only_active': True}
    sources = _get_sources_for_user(context, data_dict)

    for source in sources:
        data_dict = {'source_id': source.id}
        try:
            get_action('harvest_job_create')(context, data_dict)
        except HarvestJobExists, e:
            log.info('Trying to rerun job for %s skipping' % source.id)

        source.next_run = _caluclate_next_run(source.frequency)
        source.save()

def harvest_jobs_run(context,data_dict):
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run',context,data_dict)

    session = context['session']

    source_id = data_dict.get('source_id',None)

    if not source_id:
        _make_scheduled_jobs(context, data_dict)

    context['return_objects'] = False
	
    # Flag finished jobs as such
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'Running'})
    #jobs[0]['gather_finished']=True
    #print(jobs)
    if len(jobs):
        for job in jobs:
            if job['gather_finished']:

                
                
                objects = session.query(HarvestObject.id) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .filter(and_((HarvestObject.state!=u'COMPLETE'),
                                       (HarvestObject.state!=u'ERROR'))) \
                          .order_by(HarvestObject.import_finished.desc())

                if objects.count() == 0:
                    
                    #harmonisation job create for harvest jobs finished fetch stage
                    job_source_id=job['source_id']
                    #get harvest info
                    harvest_source_info= HarvestSource.get(job['source_id'])
                    cat_url=harvest_source_info.url
                    title=harvest_source_info.title
                    db = client.odm
                    collection=db.jobs
                    document=collection.find_one({"title":title})
                    # if job exists then create harmonisation job
                    if document!=None:
                       harmonisation_job={}
                       harmonisation_job['id']=document['id']
                       harmonisation_job['cat_url']=document['cat_url']
                       try:
                          harmonised=document['harmonised']
                       except KeyError:
                          harmonised="not yet"
                       harmonisation_job['harmonised']=harmonised
                       harmonisation_job['status']="pending"
                       harmonisation_job['dates']="dates_selected"
                       harmonisation_job['catalogue_selection']=title
                       harmonisation_job['deduplication']="deduplication_selected"
                       harmonisation_job['resources']="resources_selected"
                       harmonisation_job['licenses']="licenses_selected"
                       harmonisation_job['categories']="categories_selected"
                       harmonisation_job['save']="go-harmonisation-complete"
               
               ##create harmonise job to db
                       collection_harmonise_jobs=db.harmonise_jobs
                       collection_harmonise_jobs.save(harmonisation_job)
                       job_obj = HarvestJob.get(job['id'])
                       job_obj.status = u'Finished'

                    last_object = session.query(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .filter(HarvestObject.import_finished!=None) \
                          .order_by(HarvestObject.import_finished.desc()) \
                          .first()
                    if last_object:
			try:
                        	job_obj.finished = last_object.import_finished
			except:pass
		    try:
                    	job_obj.save()
		    except:pass
                    # Reindex the harvest source dataset so it has the latest
                    # status

		    try:		
                    	get_action('harvest_source_reindex')(context,{'id': job_obj.source.id})
		    except:pass

    # resubmit old redis tasks
    resubmit_jobs()

    # Check if there are pending harvest jobs
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'New'})
    if len(jobs) == 0:
        log.info('No new harvest jobs.')
        raise Exception('There are no new harvesting jobs')

    # Send each job to the gather queue
    publisher = get_gather_publisher()
    sent_jobs = []
    for job in jobs:
        job_source_id=job['source_id']
    #get harvest info
        harvest_source_info= HarvestSource.get(job['source_id'])
        cat_url=harvest_source_info.url
        title=harvest_source_info.title
        db = client.odm
        collection=db.jobs
        document=collection.find_one({"title":title})
       
        if document!=None and len(document.keys())>4:
          document.update({"date_reharvested":datetime.datetime.now()})
          collection.save(document)
        context['detailed'] = False
        source = harvest_source_show(context,{'id':job['source_id']})
        if source['active']:
            job_obj = HarvestJob.get(job['id'])
            job_obj.status = job['status'] = u'Running'
            job_obj.save()
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()
    return sent_jobs


@logic.side_effect_free
def harvest_sources_reindex(context, data_dict):
    '''
        Reindexes all harvest source datasets with the latest status
    '''
    log.info('Reindexing all harvest sources')
    check_access('harvest_sources_reindex', context, data_dict)

    model = context['model']

    packages = model.Session.query(model.Package) \
                            .filter(model.Package.type==DATASET_TYPE_NAME) \
                            .filter(model.Package.state==u'active') \
                            .all()

    package_index = PackageSearchIndex()

    reindex_context = {'defer_commit': True}
    for package in packages:
        get_action('harvest_source_reindex')(reindex_context, {'id': package.id})

    package_index.commit()

    return True

@logic.side_effect_free
def harvest_source_reindex(context, data_dict):
    '''Reindex a single harvest source'''

    harvest_source_id = logic.get_or_bust(data_dict, 'id')
    defer_commit = context.get('defer_commit', False)

    if 'extras_as_string'in context:
        del context['extras_as_string']
    context.update({'ignore_auth': True})
    package_dict = logic.get_action('harvest_source_show')(context,
        {'id': harvest_source_id})
    log.debug('Updating search index for harvest source {0}'.format(harvest_source_id))

    # Remove configuration values
    new_dict = {}
    if package_dict.get('config'):
        config = json.loads(package_dict['config'])
        for key, value in package_dict.iteritems():
            if key not in config:
                new_dict[key] = value
    package_index = PackageSearchIndex()
    package_index.index_package(new_dict, defer_commit=defer_commit)

    return True
