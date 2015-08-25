# ckanext-harvestodm
-----------------------------
This is an adapted version of the CKAN harvester extension (https://github.com/ckan/ckanext-harvest), being used in the OpenDataMonitor project to collect metadata from datasets hosted in open data catalogues powered by CKAN.

The main modifications include:
- during catalogue registration:
- - the catalogue registration form contains two more fields to be filled in: language and country (values are selected from a drop down menu)
- - default configuration options are automatically loaded in the form
- creation and management of harvest jobs:
- - the configuration of a harvest job is stored in the MongoDB database, where the collected metadata are also kept and analyzed
- - upon completion of a harvest job, a harmonization job is configured and scheduled for execution
- during the 'gather' stage:
- - custom code was added to address the catalogues http://data.noe.gv.at and http://data.gouv.fr, which although powered by CKAN have some modifications in the provided API
- - collect some statistics (number of metadata records collected) for debugging and monitoring purposes
- during the 'fetch' stage:
- - check whether a returned JSON document is valid (and try to validate if not)
- - add some extra metadata (language, country, catalogue_url, platfrom, metadata_created, metadata_modified)
- - check whether a metadata record is already present in the MongoDB database, and accordingly create or update
- - collect some statistics (number of metadata records collected) for debugging and monitoring purposes

Licence
----------
This work is derived from the CKAN harvester extension (https://github.com/ckan/ckanext-harvest) and thus licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).
