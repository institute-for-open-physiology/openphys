function y=nanmean(x,varargin)
y=mean(x,varargin{:},'omitnan');
end
