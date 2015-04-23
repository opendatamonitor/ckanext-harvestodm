from setuptools import setup, find_packages
import sys, os

version = '0.2'

setup(
	name='ckanext-harvestodm',
	version=version,
	description="Harvesting interface plugin for CKAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN',
	author_email='ckan@okfn.org',
	url='http://ckan.org/wiki/Extensions',
	license='mit',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.harvestodm'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
	        # dependencies are specified in pip-requirements.txt 
	        # instead of here
	],
	tests_require=[
		'nose',
		'mock',
	],
	test_suite = 'nose.collector',
	entry_points=\
	"""
    [ckan.plugins]
	# Add plugins here, eg
	harvestodm=ckanext.harvestodm.plugin:Harvest
	ckan_harvester=ckanext.harvestodm.harvesters:CKANHarvester
	test_harvester=ckanext.harvestodm.tests.test_queue:TestHarvester
	[paste.paster_command]
	harvester = ckanext.harvestodm.commands.harvester:Harvester
	""",
)
