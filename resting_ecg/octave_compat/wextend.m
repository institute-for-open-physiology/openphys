function y=wextend(dim,mode,x,n,side)
% Narrow compatibility adapter: 1-D zero-order endpoint extension only.
assert(dim==1 && strcmp(mode,'sp0') && isvector(x));
assert(n>=0 && n==round(n));
wasrow=isrow(x); x=x(:);
if strcmp(side,'l'), y=[repmat(x(1),n,1);x];
elseif strcmp(side,'r'), y=[x;repmat(x(end),n,1)];
elseif strcmp(side,'b'), y=[repmat(x(1),n,1);x;repmat(x(end),n,1)];
else error('Unsupported extension side'); end
if wasrow,y=y';end
end
