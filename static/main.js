function main() {
    document.addEventListener('fx:config', (event) => {
        event.detail.cfg.sseReconnect = true;
        event.detail.cfg.ssePauseOnHidden = true;
    });
    let countEl = document.getElementById('count')
    if (countEl) {
    } else {
        countEl = document.querySelector('#count')
    }
    countEl.addEventListener('fx:sse:error', (event) => {
        console.log(event)
        document.getElementById('tick').innerHTML = ''
    });
    countEl.addEventListener('fx:sse:close', (event) => {
        document.getElementById('tick').innerHTML = ''
    });
}

document.onload(main)
