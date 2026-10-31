const byId = id => document.getElementById(id);
function reviewStatus(){
  const symptoms=byId('symptoms').value,ecg=byId('ecg-review').value,decision=byId('decision').value;
  const reviewed=symptoms!=='unknown'&&ecg!=='unknown'&&byId('reviewer').value.trim().length>0;
  const urgent=symptoms==='urgent'||ecg==='urgent'||runData.astra_priority==='urgent_assessment';
  const proceedAllowed=reviewed&&symptoms==='negative'&&ecg==='acceptable'&&runData.model_complete&&runData.astra_priority==='routine_review'&&!runData.repeatability_unresolved;
  const valid=reviewed&&decision!=='pending'&&(urgent?decision==='urgent':decision==='proceed'?proceedAllowed:decision==='defer'||decision==='repeat'||decision==='urgent');
  byId('save-review').disabled=!valid;
  byId('letter-button').disabled=!(valid&&decision==='defer'&&!urgent);
  byId('review-state').textContent=urgent?'Urgent assessment pathway: do not start exercise. Follow the local escalation protocol.':valid?'Decision ready to export. Nothing has been saved or sent.':decision==='proceed'&&!proceedAllowed?'Proceed is unavailable until the symptom screen, ECG review and model findings have been resolved.':'Complete symptom and ECG review, name the reviewer, and choose the next step.';
  byId('gp-letter').hidden=true;
  return valid;
}
for(const id of ['symptoms','ecg-review','decision','reviewer','review-notes'])byId(id).addEventListener('input',reviewStatus);
byId('save-review').addEventListener('click',()=>{
  if(!reviewStatus())return;
  const d={...runData,recorded_at:new Date().toISOString(),reviewer:byId('reviewer').value.trim(),symptoms:byId('symptoms').value,ecg_review:byId('ecg-review').value,decision:byId('decision').value,notes:byId('review-notes').value,demo:true};
  const a=document.createElement('a');const url=URL.createObjectURL(new Blob([JSON.stringify(d,null,2)],{type:'application/json'}));a.href=url;a.download='operator-review.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
});
byId('letter-button').addEventListener('click',()=>{
  if(!reviewStatus()||byId('decision').value!=='defer')return;
  byId('letter-notes').textContent='Operator findings and relevant symptoms: '+(byId('review-notes').value.trim()||'[Reviewer to complete before issue]');
  byId('letter-reviewer').textContent='Reviewing operator: '+byId('reviewer').value.trim();byId('gp-letter').hidden=false;byId('gp-letter').scrollIntoView({behavior:'smooth'});
});
byId('audience').addEventListener('click',()=>{document.body.classList.toggle('participant');byId('audience').textContent=document.body.classList.contains('participant')?'Operator view':'Participant view';});
byId('print-strip').addEventListener('click',()=>{document.body.classList.remove('print-letter');window.print();});
byId('print-letter').addEventListener('click',()=>{document.body.classList.add('print-letter');window.print();});
window.addEventListener('afterprint',()=>document.body.classList.remove('print-letter'));
