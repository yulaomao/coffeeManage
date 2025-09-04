console.log('Coffee CM UI loaded')

window.cmToast = (msg, type='info')=>{
	try{
		const wrap = document.getElementById('toastWrap'); if(!wrap) return alert(msg)
		const el = document.createElement('div');
		el.className = 'toast align-items-center text-bg-'+(type==='error'?'danger':type)+' border-0';
		el.setAttribute('role','alert'); el.setAttribute('aria-live','assertive'); el.setAttribute('aria-atomic','true');
		el.innerHTML = `<div class="d-flex"><div class="toast-body">${msg}</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button></div>`
		wrap.appendChild(el)
		const t = new bootstrap.Toast(el,{delay:3000}); t.show(); el.addEventListener('hidden.bs.toast',()=>el.remove())
	}catch(e){ try{ alert(msg) }catch{} }
}

window.cmConfirm = async (msg)=>{
	return confirm(msg)
}
