
import os, os.path as osp, glob
import numpy as np
import tqdm
import uptools, seutils
from contextlib import contextmanager
import argparse


class Bunch:
    def __init__(self, **kwargs):
        self.arrays = kwargs

    def __getattr__(self, name):
       return self.arrays[name]

    def __getitem__(self, where):
        """Selection mechanism"""
        new = self.__class__()
        new.arrays = {k: v[where] for k, v in self.arrays.items()}
        return new

    def __len__(self):
        for k, v in self.arrays.items():
            try:
                return len(v)
            except TypeError:
                return 1


class FourVectorArray:
    """
    Wrapper class for Bunch, with more specific 4-vector stuff
    """
    def __init__(self, pt, eta, phi, energy, **kwargs):
        self.bunch = Bunch(
            pt=pt, eta=eta, phi=phi, energy=energy, **kwargs
            )

    def __getattr__(self, name):
       return getattr(self.bunch, name)

    def __getitem__(self, where):
        new = self.__class__([], [], [], [])
        new.bunch = self.bunch[where]
        return new

    def __len__(self):
        return len(self.bunch)

    @property
    def px(self):
        return np.cos(self.phi) * self.pt

    @property
    def py(self):
        return np.sin(self.phi) * self.pt

    @property
    def pz(self):
        return np.sinh(self.eta) * self.pt


def is_array(a):
    """
    Checks if a thing is an array or maybe a number
    """
    try:
        shape = a.shape
        return len(shape) >= 1
    except AttributeError:
        return False


def calc_dphi(phi1, phi2):
    """
    Calculates delta phi. Assures output is within -pi .. pi.
    """
    twopi = 2.*np.pi
    # Map to 0..2pi range
    dphi = (phi1 - phi2) % twopi
    # Map pi..2pi --> -pi..0
    if is_array(dphi):
        dphi[dphi > np.pi] -= twopi
    elif dphi > np.pi:
        dphi -= twopi
    return dphi


def calc_dr(eta1, phi1, eta2, phi2):
    return np.sqrt((eta1-eta2)**2 + calc_dphi(phi1, phi2)**2)


def calculate_mt_rt(jets, met, metphi):
    met_x = np.cos(metphi) * met
    met_y = np.sin(metphi) * met
    jet_x = np.cos(jets.phi) * jets.pt
    jet_y = np.sin(jets.phi) * jets.pt
    jet_e = np.sqrt(jets.energy**2 - jets.pz**2)
    mt = np.sqrt( (jet_e + met)**2 - (jet_x + met_x)**2 - (jet_y + met_y)**2 )
    rt = np.sqrt(1+ met / jets.pt)
    return mt, rt

def calculate_mt(jets, met, metphi):
    metx = np.cos(metphi) * met
    mety = np.sin(metphi) * met
    jets_transverse_e = np.sqrt(jets.energy**2 - jets.pz**2)
    mass = np.sqrt(jets.energy**2 - jets.px**2 - jets.py**2 - jets.pz**2)
    mt = np.sqrt(
        (jets_transverse_e + met)**2
        - (jets.px + metx)**2 - (jets.py + mety)**2
        )
    return mt

def calculate_mass(jets):
    mass = np.sqrt(jets.energy**2 - jets.px**2 - jets.py**2 - jets.pz**2)
    return mass

def calculate_massmet(jets, met, metphi):
    metx = np.cos(metphi) * met
    mety = np.sin(metphi) * met
    mass_viz = np.sqrt(jets.energy**2 - jets.px**2 - jets.py**2 - jets.pz**2)
    metdphi = calc_dphi(jets.phi, metphi)
    massmet = np.sqrt(mass_viz**2 + 2 * met * np.sqrt(jets.pz**2 + jets.pt**2 + mass_viz**2) - 2 * jets.pt * met * np.cos(metdphi))
    return massmet

def calculate_massmetpz(jets, met, metphi):
    metx = np.cos(metphi) * met
    mety = np.sin(metphi) * met
    mass_viz = np.sqrt(jets.energy**2 - jets.px**2 - jets.py**2 - jets.pz**2)
    mass = np.sqrt(mass_viz**2 + 2 * np.sqrt(met**2 + (jets.pz*met/jets.pt)**2 ) * np.sqrt(jets.pz**2 + jets.pt**2 + mass_viz**2) - 2 * (jets.pt * met * np.cos(calc_dphi(metphi, jets.phi)) + jets.pz*(jets.pz*met/jets.pt)))
    return mass

def calculate_massmetpzm(jets, met, metphi):
    metx = np.cos(metphi) * met
    mety = np.sin(metphi) * met
    mass_viz = np.sqrt(jets.energy**2 - jets.px**2 - jets.py**2 - jets.pz**2)
    mass = np.sqrt(2*mass_viz**2 +2*np.sqrt(met**2+(jets.pz*met/jets.pt)**2+mass_viz**2)*np.sqrt(jets.pz**2+jets.pt**2+mass_viz**2)-2*(jets.pt*met*np.cos(calc_dphi(metphi, jets.phi))+jets.pz*(jets.pz*met/jets.pt)))
    return mass

class CutFlowColumn:
    def __init__(self) -> None:
        self.counts = {}

    def plus_one(self, name):
        self.counts.setdefault(name, 0)
        self.counts[name] += 1

    def __getitem__(self, name):
        return self.counts.get(name, 0)

def preselection(event, cut_flow=None):
    if cut_flow is None: cut_flow = CutFlowColumn()

    if len(event[b'JetsAK15.fCoordinates.fPt']) < 2:
        return False
    cut_flow.plus_one('>=2jets')

    if abs(event[b'JetsAK15.fCoordinates.fEta'][1]) > 2.4:
          return False
    cut_flow.plus_one('eta<2.4')

    if len(event[b'JetsAK8.fCoordinates.fPt']) == 0 or event[b'JetsAK8.fCoordinates.fPt'][0] < 550.:
        return False
    cut_flow.plus_one('trigger')

    for ecf in [
        b'JetsAK15_ecfC2b1',
        b'JetsAK15_ecfD2b1',
        b'JetsAK15_ecfM2b1',
        b'JetsAK15_ecfN2b2',
        ]:
        try:
            if event[ecf][1] < 0.:
                return False
        except IndexError:
            return False
    cut_flow.plus_one('ecf>0')


    if np.sqrt(1.+event[b'MET']/event[b'JetsAK15.fCoordinates.fPt'][1]) < 1.1:
        return False
    cut_flow.plus_one('rtx>1.1')

    if event[b'Muons'] > 0 or event[b'Electrons'] > 0:
        return False
    cut_flow.plus_one('nleptons==0')

    if any(event[b] == 0 for b in [
        b'HBHENoiseFilter',
        b'HBHEIsoNoiseFilter',
        b'eeBadScFilter',
        b'ecalBadCalibReducedFilter',
        b'BadPFMuonFilter',
        b'BadChargedCandidateFilter',
        b'globalSuperTightHalo2016Filter',
        ]):
        return False
    cut_flow.plus_one('metfilter')
    #if abs(calc_dphi(event[b'JetsAK15.fCoordinates.fPhi'][1], event[b'METPhi']))>0.5:
    #    return False
    #cut_flow.plus_one('dphicut')
    #if event[b'JetsAK15.fCoordinates.fPt'][1]<250:
    #    return False
    #cut_flow.plus_one('ptcut')
    cut_flow.plus_one('preselection')
    return True


def get_subl(event):
    """
    Returns subleading jet
    """
    jets = FourVectorArray(
        event[b'JetsAK15.fCoordinates.fPt'],
        event[b'JetsAK15.fCoordinates.fEta'],
        event[b'JetsAK15.fCoordinates.fPhi'],
        event[b'JetsAK15.fCoordinates.fE'],
        ecfC2b1 = event[b'JetsAK15_ecfC2b1'],
        ecfC2b2 = event[b'JetsAK15_ecfC2b2'],
        ecfC3b1 = event[b'JetsAK15_ecfC3b1'],
        ecfC3b2 = event[b'JetsAK15_ecfC3b2'],
        ecfD2b1 = event[b'JetsAK15_ecfD2b1'],
        ecfD2b2 = event[b'JetsAK15_ecfD2b2'],
        ecfM2b1 = event[b'JetsAK15_ecfM2b1'],
        ecfM2b2 = event[b'JetsAK15_ecfM2b2'],
        ecfM3b1 = event[b'JetsAK15_ecfM3b1'],
        ecfM3b2 = event[b'JetsAK15_ecfM3b2'],
        ecfN2b1 = event[b'JetsAK15_ecfN2b1'],
        ecfN2b2 = event[b'JetsAK15_ecfN2b2'],
        ecfN3b1 = event[b'JetsAK15_ecfN3b1'],
        ecfN3b2 = event[b'JetsAK15_ecfN3b2'],
        multiplicity = event[b'JetsAK15_multiplicity'],
        girth = event[b'JetsAK15_girth'],
        ptD = event[b'JetsAK15_ptD'],
        axismajor = event[b'JetsAK15_axismajor'],
        axisminor = event[b'JetsAK15_axisminor'],
        sdm = event[b'JetsAK15_softDropMass']
        )
    subl = jets[1]
    metdphi = calc_dphi(subl.phi, event[b'METPhi'])
    subl.rt = np.sqrt(1+ event[b'MET']/subl.pt)
    subl.metdphi = calc_dphi(subl.phi, event[b'METPhi'])
    #subl.mt = calculate_mt(subl, event[b'MET'], metdphi) # this is really wrong; its making the mt baaaaad
    subl.mt = calculate_mt(subl, event[b'MET'], event[b'METPhi'])
    subl.mass = calculate_mass(subl)
    return subl

def part_flavor(event):
    min_dr = 1000
    jets = FourVectorArray(
      event[b'Jets.fCoordinates.fPt'],
      event[b'Jets.fCoordinates.fEta'],
      event[b'Jets.fCoordinates.fPhi'],
      event[b'Jets.fCoordinates.fE'],
      partonFlovor = event[b'Jets_partonFlavor'],
    )
    ak4jets = jets
    subl = get_subl(event)
    l = 1000
    for j in range(len(jets)):
      com_dr = calc_dr(ak4jets[j].eta, ak4jets[j].phi, subl.eta, subl.phi)
      if com_dr < min_dr:
        min_dr = com_dr
        l = j
    ak4partFlav = jets[j]
    return ak4partFlav

def Offset_Constituents(event):
    jets = FourVectorArray(
      event[b'JetsAK15_constituents.fCoordinates.fPt'],
      event[b'JetsAK15_constituents.fCoordinates.fEta'],
      event[b'JetsAK15_constituents.fCoordinates.fPhi'],
      event[b'JetsAK15_constituents.fCoordinates.fE'],
      offset_constituents = event[b'JetsAK15_constituentsOffsets'],
    )
    jetsconst = jets[2]
    return jetsconst

def process_signal(rootfiles, outfile=None):
    n_total = 0
    n_presel = 0
    n_final = 0
    X = []
    for event in uptools.iter_events(rootfiles):
        n_total += 1
        if not preselection(event): continue
        n_presel += 1
        genparticles = FourVectorArray(
            event[b'GenParticles.fCoordinates.fPt'],
            event[b'GenParticles.fCoordinates.fEta'],
            event[b'GenParticles.fCoordinates.fPhi'],
            event[b'GenParticles.fCoordinates.fE'],
            pdgid=event[b'GenParticles_PdgId'],
            status=event[b'GenParticles_Status']
            )

        zprime = genparticles[genparticles.pdgid == 4900023]
        if len(zprime) == 0: continue
        zprime = zprime[0]

        dark_quarks = genparticles[(np.abs(genparticles.pdgid) == 4900101) & (genparticles.status == 71)]
        if len(dark_quarks) != 2: continue
        subl = get_subl(event)
        ak4partFlav = part_flavor(event)
        jetsconst = Offset_Constituents(event)
        met = event[b'MET']

        # Verify zprime and dark_quarks are within 1.5 of the jet
        if not all(calc_dr(subl.eta, subl.phi, obj.eta, obj.phi) < 1.5 for obj in [
            zprime, dark_quarks[0], dark_quarks[1]
            ]):
            continue

        n_final += 1

        '''X.append([
            subl.girth, subl.axismajor, subl.axisminor, subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1, subl.ecfN2b2, subl.metdphi, subl.ptD, subl.multiplicity, jetsconst.offset_constituents,
            ak4partFlav.partonFlovor,
            subl.pt, subl.eta, subl.phi, subl.energy, subl.rt, subl.mt, met, subl.sdm,
            ])'''
        X.append([
            subl.girth, subl.axisminor, subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1, subl.ecfN2b2, subl.metdphi, subl.ptD, subl.multiplicity, subl.axismajor, jetsconst.offset_constituents,
            ak4partFlav.partonFlovor,
            subl.pt, subl.eta, subl.phi, subl.energy, subl.rt, subl.mt, met, subl.sdm, subl.mass,
            ])


        '''X.append([
            subl.ptD, subl.axismajor, subl.multiplicity,
            subl.girth, subl.axisminor, subl.metdphi,
            subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1, subl.ecfN2b2, ak4partFlav.partonFlovor,
            subl.pt, subl.eta, subl.phi, subl.energy, subl.rt, subl.mt, met, subl.sdm
            ])'''

    print(f'n_total: {n_total}; n_presel: {n_presel}; n_final: {n_final} ({100.*n_final/float(n_total):.2f}%)')

    if outfile is None: outfile = 'data/signal.npz'
    outdir = osp.abspath(osp.dirname(outfile))
    if not osp.isdir(outdir): os.makedirs(outdir)
    print(f'Saving {n_final} entries to {outfile}')
    np.savez(outfile, X=X)


def process_bkg(rootfiles, outfile=None, chunked_save=None, nmax=None):
#def process_bkg(rootfiles, outfile=None):
    n_total_all = 0
    n_presel_all = 0
    for rootfile in uptools.format_rootfiles(rootfiles):
        print('first debug in process_bkg')
        X = []
        n_total_this = 0
        n_presel_this = 0
        try:
            for event in uptools.iter_events(rootfile):
                n_total_this += 1
                n_total_all += 1
                if not preselection(event): continue
                n_presel_this += 1
                n_presel_all += 1
                subl = get_subl(event)
                ak4partFlav = part_flavor(event)
                X.append([
                   subl.ptD, subl.axismajor, subl.multiplicity,
                   subl.girth, subl.axisminor, subl.metdphi, 
                   subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1, subl.ecfN2b2, ak4partFlav.partonFlovor,
                   subl.pt, subl.eta, subl.phi, subl.energy, subl.rt, subl.mt
                   ])
        except IndexError:
            if n_presel_this == 0:
                print(f'Problem with {rootfile}; no entries, skipping')
                continue
            else:
                print(f'Problem with {rootfile}; saving {n_presel_this} good entries')

        outfile = 'data/bkg/{}.npz'.format(dirname_plus_basename(rootfile).replace('.root', ''))
        print(f'n_total: {n_total_this}; n_presel: {n_presel_this} ({(100.*n_presel_this)/n_total_this:.2f}%)')
        outdir = osp.abspath(osp.dirname(outfile))
        if not osp.isdir(outdir): os.makedirs(outdir)
        print(f'Saving {n_presel_this} entries to {outfile}')
        np.savez(outfile, X=X)



def dirname_plus_basename(fullpath):
    return f'{osp.basename(osp.dirname(fullpath))}/{osp.basename(fullpath)}'

@contextmanager
def make_local(rootfile):
    """Copies rootfile to local, and removes when done"""
    tmpfile = f'tmp/{dirname_plus_basename(rootfile)}'
    seutils.cp(rootfile, tmpfile)
    try:
        yield tmpfile
    finally:
        print(f'Removing {tmpfile}')
        os.remove(tmpfile)


def iter_rootfiles_umd(rootfiles):
    for rootfile in rootfiles:
        with make_local(rootfile) as tmpfile:
            yield tmpfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', type=str, choices=['signal', 'bkg', 'signal_local'])
    args = parser.parse_args()

    if args.signal_local:
        process_signal(
            list(sorted(glob.iglob('raw_signal/*.root')))
            )
    elif args.signal:
        process_signal(
            iter_rootfiles_umd(
                seutils.ls_wildcard(
                    'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/BKG/sig_mz250_rinv0p3_mDark20_Mar31/*.root'
                    )
                + ['gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/thomas.klijnsma/qcdtest3/sig_ECF_typeCDMN_Jan29/1.root']
                ),
            )
    elif args.bkg:
        process_bkg(
            iter_rootfiles_umd(seutils.ls_wildcard(
                'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/BKG/bkg_May04_year2018/*/*.root'
                )),
            )

if __name__ == '__main__':
    main()
