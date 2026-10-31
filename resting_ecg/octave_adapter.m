args=argv();dep=args{1};load(args{2});dest=args{3};tool=args{4};
pkg load signal;
if strcmp(tool,'wavedet')
  kit=fullfile(dep,'vendor','ecg-kit');addpath(kit);addpath(fullfile(kit,'common'));addpath(fullfile(kit,'common','wavedet'));
  header=struct('freq',fs,'nsig',1,'nsamp',length(signal),'gain',1,'adczero',0,'units','mV','desc','H10 ECG');
  [multi,single]=wavedet_interface(signal,header,[],1,[],[],1,[]);
else
  pkg load image;pkg load statistics;
  addpath(genpath(fullfile(dep,'vendor','ECGdeli')));addpath(args{5});
  if strcmp(tool,'ecgdeli_positive')
    single=QRS_Detection(signal,fs,'peaksQRS');single=T_Detection(signal,fs,single);single=P_Detection(signal,fs,single);
  else
    [multi,single]=Annotate_ECG_Multi(signal,fs);
  end
end
save('-mat7-binary',dest,'single');
