# pylint: disable=bad-indentation
# pylint: disable=line-too-long
# pylint: disable=unnecessary-semicolon
# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=multiple-statements
# pylint: disable=bare-except
# pylint: disable=multiple-imports
# pylint: disable=missing-class-docstring
# pylint: disable=no-self-argument
# pylint: disable=no-member
# pylint: disable=consider-using-f-string
# pylint: disable=invalid-name
# pylint: disable=unspecified-encoding
# pylint: disable=wrong-import-order
# pylint: disable=too-many-public-methods
# pylint: disable=singleton-comparison
# pylint: disable=too-many-locals
# pylint: disable=too-many-function-args
# pylint: disable=broad-exception-caught
# pylint: disable=trailing-newlines
# pylint: disable=missing-timeout
# pylint: disable=subprocess-run-check
# pylint: disable=too-many-nested-blocks
# pylint: disable=too-many-branches

import kfp, json, yaml, re, docker, sys, subprocess
from google.oauth2 import service_account
import google.auth.transport.requests as gauthreq
#from datetime import datetime
#import copy, requests, difflib, os


class KfpMigrate:

    configs = None; srcClient = None; dstClient = None
    examplePipelines = ["[Demo]","[Tutorial]"]

    def __init__(self, configs):
        self.configs = configs
        if self.configs["source"]["kfp"]["type"] == "gcp":
            credentials = service_account.Credentials.from_service_account_file(
                self.configs["source"]["kfp"]["service_account"], scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            credentials.refresh(gauthreq.Request())
            token = credentials.token
            rhost = "https://" + self.configs["source"]["kfp"]["host"] + "/pipeline"
            self.srcClient = kfp.Client(host=rhost, existing_token=token)
        elif self.configs["destination"]["kfp"]["type"] == "agnostic":
            rhost = "https://" + self.configs["destination"]["kfp"]["host"] + "/pipeline"
            self.dstClient = kfp.Client(host=rhost)
        else:
            print("Unknown kfp type")
            sys.exit(2)

    def exec(self, operation, target):
        if operation == 'migrate':
            if target == 'experiments':
                self.migrateExperiments()
            elif target == 'pipelines':
                self.migratePipelines('')
            elif target == 'jobs':
                self.migrateJobs('')
            elif target == 'runs':
                self.migrateRuns('')
        elif operation == 'delete':
            if target == 'experiments':
                self.deleteExperiments()
            elif target == 'pipelines':
                self.deletePipelines()
            elif target == 'jobs':
                self.deleteJobs()
            elif target == 'runs':
                self.deleteRuns()
        #print("pipeline-name: {}".format(findPipelineById("3e7ea0e3-6705-434c-ba53-b3fd9b1ddb63")))

    def saveFixYaml(self, pname, syml):
        srcdkrepo = self.configs["source"]["docker"]["repository"];
        srcimgname=""; dstimgname=""; dstdkrepo = self.configs["destination"]["docker"]["repository"]

        ntag=""
        try:
            ntag = re.search("{}/(.+?)\n".format(srcdkrepo), syml).group(1)
        except:  pass

        if ntag != "":
            syml = syml.replace(srcdkrepo, dstdkrepo)
            srcimgname = "{}/{}".format(srcdkrepo, ntag)
            dstimgname = "{}/{}".format(dstdkrepo, ntag)
            print(" image-src: {}".format(srcimgname))
            print(" image-dst: {}".format(dstimgname))
            cr = subprocess.run(["docker", "manifest", "inspect", dstimgname], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            scr = cr.stdout.decode('utf-8'); jcr = None
            try:
                jcr = json.loads(scr)
                _ = len(jcr['layers'])
            except: jcr = None
            if jcr is None:
                try:
                    dkclient = docker.from_env()
                    print("  docker pull ...")
                    srcimage = dkclient.images.pull(srcimgname)
                    srcimage.tag(dstimgname)
                    print("  docker push ...")
                    dkclient.images.push(dstimgname)
                    print("  remove local images ...")
                    dkclient.images.remove(srcimgname)
                    dkclient.images.remove(dstimgname)
                except docker.errors.APIError as e:
                    print(" e:{}".format(e))
            else:
                print("  image already pushed")

        fname = "pipelines/{}.yaml".format(pname)
        with open(fname, 'w') as f: f.write("{}".format(syml))
        #print(" yaml: {}".format(fname))
        return fname

    def migratePipelines(self, nextpgtkn=''):
        totcnt=0
        print("migrating pipelines ...")
        while True:
            lp = self.srcClient.list_pipelines(page_size=10, page_token=nextpgtkn)
            if lp.pipelines is None: break
            for p in lp.pipelines:
                print("{}".format(p.name))
                if any(x in p.name for x in self.examplePipelines): print(" ignored"); continue
                ttotcnt=0; nnextpgtkn=''
                while True:
                    lv = self.srcClient.list_pipeline_versions(p.id, page_size=10, page_token=nnextpgtkn)
                    if lv.versions is None: break
                    plid=''
                    for v in lv.versions:
                        if p.id != v.id: continue
                        z = self.srcClient.pipelines.get_pipeline_version_template(v.id)
                        ym = yaml.safe_dump(json.loads(z.template), default_flow_style=False)
                        fnym = self.saveFixYaml(v.name, ym)
                        rsp = self.dstClient.pipeline_uploads.upload_pipeline(fnym, name=v.name, description=p.description)
                        plid = rsp.id
                        print(" 1:{} {} {}".format(plid, rsp.name, rsp.description))
                    #print("plid:{}".format(plid))
                    if plid != '':
                        pcnt=2
                        for v in lv.versions:
                            if p.id == v.id: continue
                            z = self.srcClient.pipelines.get_pipeline_version_template(v.id)
                            ym = yaml.safe_dump(json.loads(z.template), default_flow_style=False)
                            fnym = self.saveFixYaml(v.name, ym)
                            rsp = self.dstClient.pipeline_uploads.upload_pipeline_version(fnym, name=v.name, pipelineid=plid, description=v.description)
                            print(" {}:{} {} {}".format(pcnt, rsp.id, rsp.name, rsp.description))
                            pcnt+=1
                    #break
                    ttotcnt += len(lv.versions) #lv.total_size
                    nnextpgtkn=lv.next_page_token
                    if nnextpgtkn is None: break
                print(" versions:{}".format(ttotcnt))
            totcnt += len(lp.pipelines) #lp.total_size
            nextpgtkn=lp.next_page_token
            if nextpgtkn is None: break
        print("total:{}".format(totcnt))

    ################################################################################
    ## experiments
    def migrateExperiments(self, nextpgtkn=''):
        totcnt=0
        print("migrating experiments ...")
        while True:
            le = self.srcClient.list_experiments(page_size=10, page_token=nextpgtkn)
            if le.experiments is None: break
            for e in le.experiments:
                print("{} {} {}".format(e.id, e.name, e.description))
                try:
                    self.dstClient.create_experiment(name=e.name, description=e.description)
                except: pass
            totcnt += len(le.experiments) #le.total_size
            nextpgtkn=le.next_page_token
            if nextpgtkn is None: break
        print("total:{}".format(totcnt))

    ################################################################################
    ## runs
    def getExperimentIdByName(self, client, exname):
        print(" find experiment {}".format(exname))
        exid=''; nextpgtkn=''
        while True:
            le = client.list_experiments(page_size=10, page_token=nextpgtkn)
            if le.experiments is None: break
            for e in le.experiments:
                if e.name == exname:
                    exid = e.id; break
            if exid != "": break
            nextpgtkn=le.next_page_token
            if nextpgtkn is None: break
        print(" found experiment {}".format(exid))
        return exid

    def getPipelineIdByName(self, client, plname):
        print(" find pipeline {}".format(plname))
        plid=''; nextpgtkn=''
        while True:
            lp = client.list_pipelines(page_size=10, page_token=nextpgtkn)
            if lp.pipelines is None: break
            for p in lp.pipelines:
                if p.name == plname:
                    plid = p.id; break
            if plid != "": break
            nextpgtkn=lp.next_page_token
            if nextpgtkn is None: break
        print(" found pipeline {}".format(plid))
        return plid

    def getPipelineVersionIdByName(self, client, plid, pvname):
        print(" find pipeline version {} with id {}".format(pvname, plid))
        vpid=''; nextpgtkn=''
        while True:
            lv = client.list_pipeline_versions(plid, page_size=10, page_token=nextpgtkn)
            if lv.versions is None: break
            for v in lv.versions:
                if v.name == pvname:
                    vpid = v.id; break
            if vpid != "": break
            nextpgtkn=lv.next_page_token
            if nextpgtkn is None: break
        print(" found pipeline version {}".format(vpid))
        return vpid

    def getRunIdByName(self, client, rname):
        print(" find run {}".format(rname))
        rid=''; nextpgtkn=''
        while True:
            lr = client.list_runs(page_size=10, page_token=nextpgtkn)
            if lr.runs is None: break
            for r in lr.runs:
                if r.name == rname:
                    rid = r.id; break
            if rid != "": break
            nextpgtkn=lr.next_page_token
            if nextpgtkn is None: break
        print(" found run {}".format(rid))
        return rid

    def migrateRuns(self, nextpgtkn=''):
        print("migrating runs ...")
        print("  after the 'run' created, it is automatically runs on the destination kfp")
        print("  enable below if you sure what you are doing or have some customs")
        # totcnt=0
        # while True:
        #     lt = self.srcClient.list_runs(page_size=10, page_token=nextpgtkn)
        #     if lt.runs is None: break
        #     for t in lt.runs:
        #         print("{} {}".format(t.id, t.name))
        #         # if t.status == "Running":
        #         #     print(" ignored: not accept a running run")
        #         #     continue
        #         # cek dulu run udah pernah dibuat belum
        #         drid = self.getRunIdByName(self.dstClient, t.name)
        #         if drid != "":
        #             print(" ignored: run created already")
        #             continue
        #         t.pipeline_spec.pipeline_id = self.getPipelineIdByName(self.dstClient, t.pipeline_spec.pipeline_name)
        #         if t.pipeline_spec.pipeline_id == "":
        #             print(" failed: pipeline not exist")
        #             continue
        #         expempty=False; plvempty=False
        #         for h in t.resource_references:
        #             if h.key.type == "EXPERIMENT":
        #                 h.key.id = self.getExperimentIdByName(self.dstClient, h.name)
        #                 if h.key.id == "": expempty = True
        #             elif h.key.type == "PIPELINE_VERSION":
        #                 h.key.id = self.getPipelineVersionIdByName(self.dstClient, t.pipeline_spec.pipeline_id, h.name)
        #                 if h.key.id == "": plvempty = True
        #         if expempty:
        #             print(" failed: experiment id was empty")
        #             continue
        #         if plvempty:
        #             print(" failed: pipeline version id was empty")
        #             continue
        #         rsp = None
        #         try:
        #             rsp = self.dstClient.runs.create_run(t)
        #             print(" run create success!")
        #         except Exception as e:
        #             print("failed:e1:{}".format(e))
        #         try:
        #             if rsp.run.status == "Running":
        #                 self.dstClient.runs.terminate_run(rsp.run.id)
        #             print(" terminate run success!")
        #         except Exception as e:
        #             print("failed:e2:{}".format(e))
        #     totcnt += len(lt.runs) #lt.total_size
        #     nextpgtkn=lt.next_page_token
        #     if nextpgtkn is None: break
        #     print("nextpgtkn: {}".format(nextpgtkn))
        #     #break
        # print("total:{}".format(totcnt))

    ################################################################################
    ## recurring runs
    def getJobIdByName(self, client, jname):
        print(" find job {}".format(jname))
        jid=''; nextpgtkn=''
        while True:
            lj = client.list_recurring_runs(page_size=10, page_token=nextpgtkn)
            if lj.jobs is None: break
            for j in lj.jobs:
                if j.name == jname:
                    jid = j.id; break
            if jid != "": break
            nextpgtkn=lj.next_page_token
            if nextpgtkn is None: break
        print(" found job {}".format(jid))
        return jid

    def getPipelineVersionIdByPipelineVersionName(self, client, pvname):
        print(" find pipeline version {}".format(pvname))
        vpid=''; nextpgtkn=''
        while True:
            lp = client.list_pipelines(page_size=10, page_token=nextpgtkn)
            if lp.pipelines is None: break
            for p in lp.pipelines:
                nnextpgtkn=''
                while True:
                    lv = client.list_pipeline_versions(p.id, page_size=10, page_token=nnextpgtkn)
                    if lv.versions is None: break
                    for v in lv.versions:
                        if v.name == pvname:
                            vpid = v.id; break
                    if vpid != "": break
                    nnextpgtkn=lv.next_page_token
                    if nnextpgtkn is None: break
            if vpid != "": break
            nextpgtkn=lp.next_page_token
            if nextpgtkn is None: break
        print(" found pipeline version {}".format(vpid))
        return vpid

    def migrateJobs(self, nextpgtkn=''):
        totcnt=0
        print("migrating jobs ...")
        while True:
            lj = self.srcClient.list_recurring_runs(page_size=10, page_token=nextpgtkn)
            if lj.jobs is None: break
            for j in lj.jobs:
                # print("{}".format(j))
                print("{} {}".format(j.id, j.name))
                # cek dulu job udah pernah dibuat belum
                djid = self.getJobIdByName(self.dstClient, j.name)
                if djid != "":
                    print(" ignored: job created already")
                    continue
                if j.pipeline_spec.pipeline_name is not None:
                    j.pipeline_spec.pipeline_id = self.getPipelineIdByName(self.dstClient, j.pipeline_spec.pipeline_name)
                    if j.pipeline_spec.pipeline_id == "":
                        print(" failed: pipeline not exist")
                        continue
                j.enabled = False
                expempty=False; plvempty=False
                for h in j.resource_references:
                    if h.key.type == "EXPERIMENT":
                        h.key.id = self.getExperimentIdByName(self.dstClient, h.name)
                        if h.key.id == "": expempty = True
                    elif h.key.type == "PIPELINE_VERSION":
                        if j.pipeline_spec.pipeline_name is None:
                            h.key.id = self.getPipelineVersionIdByPipelineVersionName(self.dstClient, h.name)
                        else:
                            h.key.id = self.getPipelineVersionIdByName(self.dstClient, j.pipeline_spec.pipeline_id, h.name)
                        if h.key.id == "": plvempty = True
                if expempty:
                    print(" failed: experiment id was empty")
                    continue
                if plvempty:
                    print(" failed: pipeline version id was empty")
                    continue
                try:
                    self.dstClient.jobs.create_job(j)
                    print(" job create success!")
                except Exception as e:
                    print("failed:e:{}".format(e))
            totcnt += len(lj.jobs) #lj.total_size
            nextpgtkn=lj.next_page_token
            if nextpgtkn is None: break
            print("nextpgtkn: {}".format(nextpgtkn))
            break
        print("total:{}".format(totcnt))

    ################################################################################
    ## BAHAYA: delete jobs (recurring_runs)
    def deleteJobs(self):
        print("deleting jobs ...")
        print("  enable below if you sure what you are doing or have some customs")
        # totcnt=0; nextpgtkn=''
        # while True:
        #     lj = self.dstClient.list_recurring_runs(page_size=10, page_token=nextpgtkn)
        #     if lj.jobs is None: break
        #     for j in lj.jobs:
        #         print("{} {}".format(j.id, j.name))
        #         try:
        #             self.dstClient.jobs.delete_job(j.id)
        #         except: pass
        #     totcnt += len(lj.jobs) #lj.total_size
        #     nextpgtkn=lj.next_page_token
        #     if nextpgtkn is None: break
        # print("total:{}".format(totcnt))

    def deleteRuns(self):
        print("deleting runs ...")
        print("  enable below if you sure what you are doing or have some customs")
        # totcnt=0; nextpgtkn=''
        # while True:
        #     kj = self.dstClient.list_runs(page_size=10, page_token=nextpgtkn)
        #     if kj.runs is None: break
        #     for k in kj.runs:
        #         print("{} {}".format(k.id, k.name))
        #         try:
        #             self.dstClient.runs.delete_run(k.id)
        #         except: pass
        #     totcnt += len(kj.runs) #kj.total_size
        #     nextpgtkn=kj.next_page_token
        #     if nextpgtkn is None: break
        # print("total:{}".format(totcnt))

    def deletePipelines(self):
        print("deleting pipelines ...")
        print("  enable below if you sure what you are doing or have some customs")
        # totcnt=0; nextpgtkn=''
        # while True:
        #     mp = self.dstClient.list_pipelines(page_size=10, page_token=nextpgtkn)
        #     if mp.pipelines is None: break
        #     for p in mp.pipelines:
        #         if any(x in p.name for x in self.examplePipelines): print("ignored"); continue
        #         print("{} {}".format(p.id, p.name))
        #         ttotcnt=0; nnextpgtkn=''
        #         while True:
        #             mv = self.dstClient.list_pipeline_versions(p.id, page_size=10, page_token=nnextpgtkn)
        #             if mv.versions is None: break
        #             for v in mv.versions:
        #                 print("{} {}".format(v.id, v.name))
        #                 try:
        #                     self.dstClient.delete_pipeline_version(v.id)
        #                 except: pass
        #             try:
        #                 self.dstClient.delete_pipeline(p.id)
        #             except: pass
        #             ttotcnt += len(mv.versions) #mv.total_size
        #             nnextpgtkn=mv.next_page_token
        #             if nnextpgtkn is None: break
        #         print("versions:{}".format(ttotcnt))
        #     totcnt += len(mp.pipelines) #mp.total_size
        #     nextpgtkn=mp.next_page_token
        #     if nextpgtkn is None: break
        # print("total:{}".format(totcnt))

    def deleteExperiments(self):
        print("deleting experiments ...")
        print("  enable below if you sure what you are doing or have some customs")
        # totcnt=0; nextpgtkn=''
        # while True:
        #     dp = self.dstClient.list_experiments(page_size=10, page_token=nextpgtkn)
        #     if dp.experiments is None: break
        #     for d in dp.experiments:
        #         print("{} {} {}".format(d.id, d.name, d.description))
        #         try:
        #             self.dstClient.delete_experiment(d.id)
        #         except: pass
        #     totcnt += len(dp.experiments)
        #     nextpgtkn=dp.next_page_token
        #     if nextpgtkn is None: break
        # print("total:{}".format(totcnt))

    def findPipelineById(self, plid):
        print(" find pipeline {}".format(plid))
        plname=''; nextpgtkn=''
        while True:
            lp = self.dstClient.list_pipelines(page_size=10, page_token=nextpgtkn)
            if lp.pipelines is None: break
            for p in lp.pipelines:
                nnextpgtkn=''
                while True:
                    lv = self.dstClient.list_pipeline_versions(p.id, page_size=10, page_token=nnextpgtkn)
                    for v in lv.versions:
                        if v.id == plid:
                            plname = v.name; break
                    if plname != "": break
                    nnextpgtkn=lv.next_page_token
                    if nnextpgtkn is None: break
            if plname != "": break
            nextpgtkn=lp.next_page_token
            if nextpgtkn is None: break
        print(" found pipeline {}".format(plname))
        return plname

################################################################################
## main

def main(configpath, operation, target):

    lconfigs = None
    try:
        with open(configpath, 'r') as json_file:
            lconfigs = json.load(json_file)
    except:
        print("config_path failed: {}".format(sys.exc_info()))
        sys.exit(2)

    if lconfigs is None:
        print("config_path is empty")
        sys.exit(2)

    kfpmigrate = KfpMigrate(lconfigs)
    kfpmigrate.exec(operation, target)

if __name__ == '__main__':

    argc = len(sys.argv)
    if argc < 4:
        print("python -u kfpmigrate.py [config_path] [migrate/delete] [experiments/pipelines/jobs/runs]")
        sys.exit(2)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
