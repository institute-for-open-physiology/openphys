(() => {
  const el = id => document.getElementById(id), p = ecgPlotData;
  const graph = el('ecg-plot'), primary = el('plot-method'), comparison = el('plot-compare'), beatSelect = el('plot-beat');
  const escape = text => String(text).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
  const colors = {P_on:'#a863ae',P_peak:'#723580',P_off:'#c687bf',QRS_on:'#1573ae',Q_peak:'#8f5051',R_peak:'#134d76',S_peak:'#377d97',QRS_off:'#629bd3',T_on:'#19917a',T_peak:'#00664e',T_off:'#18a34a'};
  let range=[0,10],ready=false;
  for (const m of p.methods) {
    for (const select of [primary,comparison]) { const o=document.createElement('option');o.value=m.id;o.textContent=m.name+(m.status==='ok'?'':' · failed');select.appendChild(o); }
  }
  primary.value=p.methods.some(m=>m.id==='astra')?'astra':p.methods[0].id;
  const method = select => p.methods.find(m=>m.id===select.value);
  const number = x => x===null || x===undefined?'unavailable':`${Math.round(x)} ms`;
  function populateBeats(){
    beatSelect.replaceChildren(new Option('Full 10 seconds','full'));
    method(primary).beats.forEach((b,i)=>beatSelect.add(new Option(`Beat ${i+1} · ${b.fid_s.toFixed(3)} s`,String(i))));
    beatSelect.value='full';range=[0,10];
  }
  function focused(m){
    if(beatSelect.value==='full')return null;
    const target=method(primary).beats[Number(beatSelect.value)];
    const b=m.beats.reduce((best,b)=>!best||Math.abs(b.fid_s-target.fid_s)<Math.abs(best.fid_s-target.fid_s)?b:best,null);
    return b&&Math.abs(b.fid_s-target.fid_s)<.1?b:null;
  }
  function traces(m,overlay){
    return p.landmarks.flatMap(key=>{
      const points=m.beats.flatMap((b,i)=>b.marks[key]?[{b,i,m:b.marks[key]}]:[]);
      if(!points.length)return [];
      const text=points.map(({b,i,m:a})=>`${escape(m.name)} · beat ${i+1}<br><b>${key}</b> at ${a.seconds.toFixed(6)} s<br>Nearest native sample ${a.nearest_native_index} (${a.native_time_s.toFixed(6)} s)<br>${a.range?`Visual range: ${a.range[0].toFixed(6)}–${a.range[1].toFixed(6)} s`:'No visual range supplied'}<br>Confidence: ${escape(b.confidence)}<br>${b.notes.map(escape).join('<br>')}`);
      return [{type:'scatter',mode:'markers',name:(overlay?'Overlay · ':'')+key,x:points.map(x=>x.m.seconds),y:points.map(x=>x.m.mv),text,
        customdata:points.map(x=>({beatIndex:x.i,method:m.id,key})),hovertemplate:'%{text}<br>%{y:.3f} mV<extra></extra>',
        marker:{size:overlay?10:8,color:overlay?'#da7717':colors[key],symbol:overlay?'diamond-open':key.endsWith('_on')?'triangle-up':key.endsWith('_off')?'triangle-down':'circle',line:{width:overlay?2:1,color:overlay?'#da7717':'white'}},
        error_x:{type:'data',symmetric:false,array:points.map(x=>x.m.range?x.m.range[1]-x.m.seconds:0),arrayminus:points.map(x=>x.m.range?x.m.seconds-x.m.range[0]:0),visible:el('plot-uncertainty').checked&&!overlay,color:colors[key],thickness:1,width:3}}];
    });
  }
  function draw(){
    const m=method(primary),other=method(comparison),focus=focused(m),shapes=[],annotations=[];
    const selected=[{method:m,color:'#087e70',y:.98}];if(other&&other.id!==m.id)selected.push({method:other,color:'#da7717',y:.87});
    el('plot-beat-notes').replaceChildren();
    for(const s of selected){
      const b=focused(s.method);const line=document.createElement('p');line.className='plot-reading';
      line.textContent=s.method.name+' — '+(b?`beat QT ${number(b.measurements.QT_ms)}, QRS ${number(b.measurements.QRS_ms)}, PR ${number(b.measurements.PR_ms)}. QRS onset ${b.marks.QRS_on?.seconds.toFixed(6)??'missing'} s; T end ${b.marks.T_off?.seconds.toFixed(6)??'missing'} s.`:`median QT ${number(s.method.summary.QT_ms?.median)}, QRS ${number(s.method.summary.QRS_ms?.median)}.`);
      el('plot-beat-notes').appendChild(line);
      if(b){const note=document.createElement('p');note.className='small';note.textContent=b.notes.join(' ');el('plot-beat-notes').appendChild(note);}
      if(b?.marks.QRS_on&&b?.marks.T_off&&b.measurements.QT_ms!==null&&b.measurements.QT_ms!==undefined){
        const x0=b.marks.QRS_on.seconds,x1=b.marks.T_off.seconds;
        shapes.push({type:'line',xref:'x',yref:'paper',x0,x1,y0:s.y,y1:s.y,line:{color:s.color,width:3}},
          {type:'line',xref:'x',yref:'paper',x0,x1:x0,y0:0,y1:s.y,line:{color:s.color,width:1,dash:'dot'}},
          {type:'line',xref:'x',yref:'paper',x0:x1,x1,y0:0,y1:s.y,line:{color:s.color,width:1,dash:'dot'}});
        annotations.push({xref:'x',yref:'paper',x:(x0+x1)/2,y:s.y,yanchor:'bottom',text:`${escape(s.method.name)} QT ${number(b.measurements.QT_ms)}`,font:{color:s.color,size:11},showarrow:false,bgcolor:'rgba(255,255,255,.9)'});
      }
    }
    el('plot-summary').textContent=m.status!=='ok'?`${m.name}: ${m.error||'no successful result'}. No annotation fabricated.`:focus?`Focused beat ${Number(beatSelect.value)+1}. Boundary lines show each method’s chosen QT; the waveform is never shifted to align methods.`:`${m.name}: ${m.beats.length} QRS fiducials in view. Select a beat or “Compare QT endings” to inspect boundaries.`;
    el('plot-prev').disabled=beatSelect.value==='full'||Number(beatSelect.value)<=0;
    el('plot-next').disabled=!m.beats.length||(beatSelect.value!=='full'&&Number(beatSelect.value)>=m.beats.length-1);
    const raw={type:'scatter',mode:focus?'lines+markers':'lines',name:'Native ECG',x:p.times,y:p.millivolts,line:{color:'#203746',width:1.5},marker:{size:3},customdata:p.times.map((_,i)=>i),hovertemplate:'Sample %{customdata}<br>%{x:.6f} s<br>%{y:.3f} mV<extra>Native ECG</extra>'};
    const data=[raw,...traces(m,false),...(other&&other.id!==m.id?traces(other,true):[])];
    const layout={height:510,margin:{l:55,r:20,t:46,b:105},paper_bgcolor:'white',plot_bgcolor:'white',font:{family:'system-ui',color:'#254350',size:12},
      xaxis:{title:{text:'Time from strip start (seconds)'},range,showgrid:true,gridcolor:'#edf1f4',zeroline:false},
      yaxis:{title:{text:'ECG (mV)'},range:[Math.min(...p.millivolts)-.2,Math.max(...p.millivolts)+.55],showgrid:true,gridcolor:'#edf1f4',zeroline:true,zerolinecolor:'#c3ced4'},
      legend:{orientation:'h',y:-.24,x:0,font:{size:10}},hovermode:'closest',dragmode:'zoom',shapes,annotations};
    ready=false;graph.dataset.ready='false';
    return Plotly.react(graph,data,layout,{responsive:true,displaylogo:false,scrollZoom:true,toImageButtonOptions:{format:'svg',filename:'annotated_ecg'}}).then(()=>{ready=true;graph.dataset.ready='true';});
  }
  function changeView(){
    const b=beatSelect.value==='full'?null:method(primary).beats[Number(beatSelect.value)];
    range=b?[Math.max(p.times[0],b.fid_s-.35),Math.min(p.times.at(-1),b.fid_s+.68)]:[0,10];return draw();
  }
  primary.addEventListener('change',()=>{populateBeats();draw();});comparison.addEventListener('change',draw);beatSelect.addEventListener('change',changeView);
  el('plot-uncertainty').addEventListener('change',draw);
  el('plot-full').addEventListener('click',()=>{beatSelect.value='full';changeView();});
  el('plot-prev').addEventListener('click',()=>{beatSelect.value=String(Math.max(0,Number(beatSelect.value)-1));changeView();});
  el('plot-next').addEventListener('click',()=>{beatSelect.value=beatSelect.value==='full'?'0':String(Math.min(method(primary).beats.length-1,Number(beatSelect.value)+1));changeView();});
  el('plot-qt').addEventListener('click',()=>{primary.value=p.methods.some(m=>m.id==='astra')?'astra':p.methods[0].id;populateBeats();comparison.value=p.methods.find(m=>m.name.includes('different prompt'))?.id||(p.methods.some(m=>m.id==='neurokit_dwt')?'neurokit_dwt':'');if(method(primary).beats.length)beatSelect.value='0';changeView();});
  document.addEventListener('click',event=>{const button=event.target.closest('[data-show-method]');if(!button)return;primary.value=button.dataset.showMethod;populateBeats();draw();el('annotation-review').scrollIntoView({behavior:'smooth'});});
  populateBeats();draw().then(()=>{
    graph.on('plotly_click',event=>{const d=event.points[0]?.customdata;if(d&&typeof d==='object'&&d.method===primary.value){beatSelect.value=String(d.beatIndex);changeView();}});
    graph.on('plotly_relayout',event=>{if(!ready)return;if(event['xaxis.autorange']){beatSelect.value='full';range=[0,10];draw();}else if(Array.isArray(event['xaxis.range'])){range=event['xaxis.range'].slice();}else if(event['xaxis.range[0]']!==undefined)range=[event['xaxis.range[0]'],event['xaxis.range[1]']];});
  });
})();
