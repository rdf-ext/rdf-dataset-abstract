const AbstractDataset = require('../..')

class ArrayDataset extends AbstractDataset {
  constructor (quads) {
    super()

    this._quads = []

    if (quads) {
      this.addAll(quads)
    }
  }

  _add (quad) {
    this._quads.push(quad)
  }

  _factory (quads) {
    return new ArrayDataset(quads)
  }

  _forEach (callback) {
    this._quads.forEach(quad => {
      callback.call(this, quad)
    })
  }

  _remove (quad) {
    this._quads.splice(this._quads.indexOf(quad), 1)
  }
}

module.exports = ArrayDataset
