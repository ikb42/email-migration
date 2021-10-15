async function wait(ms) {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve(true)
        }, ms)
    })
}

module.exports = {
    wait
}