(() => {
  const reveal=()=>{let id;try{id=decodeURIComponent(location.hash.slice(1));}catch{return;}if(!id)return;const note=document.getElementById(id);if(note?.matches('details.research-note-page'))note.open=true;};
  window.addEventListener('hashchange',reveal);reveal();
})();
