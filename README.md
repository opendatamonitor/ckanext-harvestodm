# ckanext-harvestodm

This is an adapted version of the CKAN harvester extension (https://github.com/ckan/ckanext-harvest), being used in the OpenDataMonitor project to collect metadata from datasets hosted in open data catalogues powered by CKAN.

## General

The ckanext-harvestodm plugin adds support in using the mongo DB as metadata repository. 
Also, changes or modifications added to original code to comply with ODM project's (www.opendatamonitor.eu) requirements (see below).

## Implementation

Main modifications are grouped as follows:

__in catalogue registration__:
- the catalogue registration form contains two more fields to be filled in: language and country (values are selected from a drop down menu)
- default configuration options are automatically loaded in the form

__creation and management of harvest jobs__:
- the configuration of a harvest job is stored in the MongoDB database, where the collected metadata are also kept and analyzed
- upon completion of a harvest job, a harmonization job is configured and scheduled for execution

__in 'gather' stage__:
- custom code was added to address the catalogues http://data.noe.gv.at and http://data.gouv.fr, which although powered by CKAN have some modifications in the provided API

__in 'fetch' stage__:
- add extra metadata fields (language, country, catalogue_url, platform) or use existing ones in different way (metadata_created and metadata_updated are synchronised to our platform's timings overriding the client's)
- check whether a metadata record is already present in the MongoDB database, and accordingly create or update


Building
---------

In order to use this extension you need to have MongoDB installed.

To build and use this plugin, simply:

    git clone https://github.com/opendatamonitor/ckanext-harvestodm.git
    cd ckanext-harvestodm
    pip install -r requirements.txt
    python setup.py develop

Then you will need to update your CKAN configuration to include the new harvester.  This will mean adding the
ckanext-harvestodm plugin as a plugin.  E.g.

    ckan.plugins = harvestodm ckan_harvester

Also you need to add the odm_extension settings to the development.ini file in your ckan folder.  

    [ckan:odm_extensions]
    mongoclient=localhost
    mongoport=27017
    log_path=/var/local/ckan/default/pyenv/src/

Using
---------

After setting this up, you should be able to go to:
    http://localhost:5000/harvest

In case that you don't have the ckanext-htmlharvest extension installed (https://github.com/opendatamonitor/ckanext-htmlharvest)
Then go to:
    http://localhost:5000/harvest/new
And have a new "CKAN" harvest type show up when creating sources.



## Licence

This work is derived from the CKAN harvester extension (https://github.com/ckan/ckanext-harvest) and thus licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).
