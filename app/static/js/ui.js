// v2 UI helpers: toast, confirm, debounce, loading overlay, theme & density toggles
(function(){
  const ls = window.localStorage
  function get(key, d){ try{ const v = ls.getItem(key); return v===null? d : JSON.parse(v) }catch{ return d } }
  function set(key, v){ try{ ls.setItem(key, JSON.stringify(v)) }catch{} }

  // theme/density
  function applyTheme(theme){ document.documentElement.setAttribute('data-theme', theme); set('cm.theme', theme) }
  function applyDensity(d){ document.body.classList.toggle('density-compact', d==='compact'); set('cm.density', d) }
  function initTheme(){ const t = get('cm.theme','light'); applyTheme(t) }
  function initDensity(){ const d = get('cm.density','comfortable'); applyDensity(d) }
  function toggleTheme(){ const cur = document.documentElement.getAttribute('data-theme')||'light'; applyTheme(cur==='light'?'dark':'light') }
  function toggleDensity(){ const cur = get('cm.density','comfortable'); applyDensity(cur==='comfortable'?'compact':'comfortable') }

  // toast
  function toast(msg, type='info', delay=2400){
    const wrap = document.getElementById('toastWrap') || document.body
    const el = document.createElement('div')
    const theme = type==='error'?'danger':(type==='success'?'success':(type==='warning'?'warning':'primary'))
    el.className = `toast align-items-center text-bg-${theme} border-0`
    el.setAttribute('role','status'); el.setAttribute('aria-live','polite')
    el.innerHTML = `<div class="d-flex"><div class="toast-body">${msg}</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button></div>`
    wrap.appendChild(el)
    const t = new bootstrap.Toast(el, {delay}); t.show(); el.addEventListener('hidden.bs.toast', ()=> el.remove())
  }

  // confirm dialog (modal)
  function confirmDialog(opts){
    const {title='确认操作', message='确定继续？', okText='确定', cancelText='取消'} = opts||{}
    const id = 'cmConfirmModal'
    let el = document.getElementById(id)
    if(!el){
      el = document.createElement('div')
      el.className='modal fade'; el.id=id; el.tabIndex=-1; el.innerHTML=`
        <div class="modal-dialog modal-dialog-centered">
          <div class="modal-content">
            <div class="modal-header"><h5 class="modal-title"></h5><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button></div>
            <div class="modal-body"></div>
            <div class="modal-footer"><button type="button" class="btn btn-secondary" data-bs-dismiss="modal"></button><button type="button" class="btn btn-primary" id="cmConfirmOK"></button></div>
          </div>
        </div>`
      document.body.appendChild(el)
    }
    el.querySelector('.modal-title').textContent = title
    el.querySelector('.modal-body').textContent = message
    el.querySelector('.btn-secondary').textContent = cancelText
    const okBtn = el.querySelector('#cmConfirmOK'); okBtn.textContent = okText
    return new Promise(resolve=>{
      const modal = new bootstrap.Modal(el)
      function cleanup(){ okBtn.removeEventListener('click', onOK); el.removeEventListener('hidden.bs.modal', onHide) }
      function onOK(){ cleanup(); modal.hide(); resolve(true) }
      function onHide(){ cleanup(); resolve(false) }
      okBtn.addEventListener('click', onOK)
      el.addEventListener('hidden.bs.modal', onHide, {once:true})
      modal.show()
    })
  }

  // debounce
  function debounce(fn, wait){ let t; return function(...args){ clearTimeout(t); t=setTimeout(()=>fn.apply(this,args), wait) } }

  // loading overlay for a container
  function withLoading(container){
    const mask = document.createElement('div')
    mask.className='ui-loading-mask'; mask.innerHTML='<div class="spinner-border text-primary" role="status"></div>'
    container.style.position = container.style.position || 'relative'
    return {
      show(){ container.appendChild(mask) },
      hide(){ try{ container.removeChild(mask) }catch{} }
    }
  }

  // expose
  window.cmUI = { toast, confirmDialog, debounce, withLoading, toggleTheme, toggleDensity, initTheme, initDensity }
  // auto init when loaded late
  if(document.readyState!=='loading'){ initTheme(); initDensity() } else { document.addEventListener('DOMContentLoaded', ()=>{ initTheme(); initDensity() }) }
})();
