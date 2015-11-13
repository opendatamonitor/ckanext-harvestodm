from pymongo import MongoClient

class HarmonizationJobs:
    def create_tedious_jobs(self,db,title,harmonisation_job={}):
        if not harmonisation_job:
            return False

        harmonisation_job['catalogue_selection']=title
        # try:
        #   harmonised=document['harmonised']
        # except KeyError:
        #   harmonised="not yet"
        harmonisation_job['status']="pending"
        harmonisation_job['for_running']=["duplicates","resources"]
        # harmonisation_job['save']="go-harmonisation-complete"

        ##create harmonise job to db
        db.harmonise_tedious_jobs.save(harmonisation_job)

        return True

    def test(self):
        client = MongoClient('127.0.0.1',27017)
        db = client['odm']

        document={}
        # document['id']='b8fff5de-6203-4a9b-bab0-67dc5ca117e3'
        # document['cat_url']='http://geodata.gov.gr'
        document['id']='a061e747-0cd8-4716-9a8f-a399cb306b30'
        document['cat_url']= 'http://data.amsterdamopendata.nl/'
        document['priority']=2
        document['all_datasets']=False

        self.create_tedious_jobs(db,'cat_new',document)

if __name__=="__main__":
    HarmonizationJobs().test()
