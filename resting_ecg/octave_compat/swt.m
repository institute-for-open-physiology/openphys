function [a,d]=swt(x,level,varargin)
% Periodic undecimated transform, checked numerically against PyWavelets1.8.
% This adapter is an experimental Octave route, not a MATLAB parity claim.
x=x(:)'; n=length(x);assert(mod(n,2^level)==0);
if length(varargin)==1
  f=load(fullfile(fileparts(mfilename('fullpath')),'wavelet_filters.mat'));
  name=strrep(varargin{1},'.','_');assert(isfield(f,[name '_lo']));
  lo=f.([name '_lo']);hi=f.([name '_hi']);
elseif length(varargin)==2
  lo=varargin{1};hi=varargin{2};
else,error('Unsupported SWT arguments');end
assert(length(lo)==length(hi)&&mod(length(lo),2)==0);
a=zeros(level,n);d=zeros(level,n); nf=length(lo);
for lev=1:level
 step=2^(lev-1); low=zeros(1,n);high=zeros(1,n);
 for j=1:nf
  shifted=circshift(x,[0,step*(j-1-nf/2)]);
  low=low+lo(j)*shifted;high=high+hi(j)*shifted;
 end
 a(lev,:)=low;d(lev,:)=high;x=low;
end
end
