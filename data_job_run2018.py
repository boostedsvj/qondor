"""# submit
# htcondor('request_memory', '4096MB')
htcondor('request_memory', '6000MB')
import seutils, os.path as osp, itertools

print('Compiling list of rootfiles...')

data_rootfiles = seutils.ls_wildcard('root://cmseos.fnal.gov//store/user/lpcsusyhad/SusyRA2Analysis2015/Run2ProductionV20/Run2018A-UL2018-v1/JetHT/*.root')

bdt_json = 'svjbdt_girthreweight_Jan06.json' #girth re-weight

def submit_chunk(chunk):
    submit(
        rootfiles=chunk,
        bdt_json=bdt_json,
        run_env='condapack:root://cmseos.fnal.gov//store/user/klijnsma/conda-svj-bdt.tar.gz',
        transfer_files=[bdt_json],
        )

for chunk in qondor.utils.chunkify(data_rootfiles, chunksize=10):
    submit_chunk(chunk)
"""# endsubmit

import qondor, seutils
import os.path as osp, os

# Download the bdtcode
os.makedirs('bdtcode/')
os.chdir('bdtcode')
github_url = 'https://raw.githubusercontent.com/boostedsvj/svj-bdt/merged/bdtcode/'
for f in [
    '__init__.py', 'crosssections.py', 'crosssections_Oct12.npz',
    'dataset.py', 'histogramming.py', 'sample.py', 'utils.py'
    ]:
    os.system('wget ' + github_url + f)
os.chdir('..')

import bdtcode

bdtcode.do_ultra_legacy() # Tell the code we're processing an UL sample

model = bdtcode.utils.get_model(qondor.scope.bdt_json)

for rootfile in qondor.scope.rootfiles:
    try:
        seutils.cp(rootfile, 'in.root')
        bdtcode.histogramming.dump_score_npz(['in.root'], model, 'out.npz', dataset_name=rootfile)

        outfile = (
            'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/UltraLegacy_npz/Data_04132022_Run2018A_UL_'
            + '/'.join(rootfile.split('/')[-3:]).replace('.root', '.npz')
            )

        seutils.cp('out.npz', outfile, env=qondor.BARE_ENV)


    except Exception as e:
        print('Failed for rootfile ' + rootfile + ':')
        print(e)
        
    finally:
        if osp.isfile('out.npz'): os.remove('out.npz')
        if osp.isfile('in.root'): os.remove('in.root')
