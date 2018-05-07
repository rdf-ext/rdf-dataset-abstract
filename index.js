const Readable = require('readable-stream')

class AbstractDataset {
  constructor (quads) {
    if (quads) {
      this.addAll(quads)
    }
  }

  _add () {
    throw new Error('_add not implemented')
  }

  _factory () {
    throw new Error('_factory not implemented')
  }

  _forEach () {
    throw new Error('_forEach not implemented')
  }

  _remove () {
    throw new Error('_remove not implemented')
  }

  get length () {
    return this.toArray().length
  }

  add (quad) {
    if (!this.includes(quad)) {
      this._add(quad)
    }

    return this
  }

  addAll (quads) {
    quads.forEach(quad => {
      this.add(quad)
    })

    return this
  }

  clone () {
    return this._factory(this)
  }

  difference (other) {
    return this._factory(this.filter(quad => {
      return !other.includes(quad)
    }))
  }

  every (callback) {
    return this.toArray().every(quad => {
      return callback(quad, this)
    })
  }

  filter (callback) {
    return this._factory(this.toArray().filter(quad => {
      return callback(quad, this)
    }))
  }

  forEach (callback) {
    this._forEach(callback)
  }

  import (stream) {
    return new Promise((resolve, reject) => {
      stream.once('end', () => {
        resolve(this)
      })

      stream.once('error', reject)

      stream.on('data', quad => {
        this.add(quad)
      })
    })
  }

  includes (quad) {
    return this.some(other => {
      return other.equals(quad)
    })
  }

  intersection (other) {
    return this._factory(this.filter(quad => {
      return other.includes(quad)
    }))
  }

  map (callback) {
    return this._factory(this.toArray().map(quad => {
      return callback(quad, this)
    }))
  }

  match (subject, predicate, object, graph) {
    return this._factory(this.filter(quad => {
      if (subject && !quad.subject.equals(subject)) {
        return false
      }

      if (predicate && !quad.predicate.equals(predicate)) {
        return false
      }

      if (object && !quad.object.equals(object)) {
        return false
      }

      if (graph && !quad.graph.equals(graph)) {
        return false
      }

      return true
    }))
  }

  merge (other) {
    return (this.clone()).addAll(other)
  }

  remove (quad) {
    this.match(quad.subject, quad.predicate, quad.object, quad.graph).forEach(matchingQuad => {
      this._remove(matchingQuad)
    })

    return this
  }

  removeMatches (subject, predicate, object, graph) {
    this.match(subject, predicate, object, graph).forEach(quad => {
      this.remove(quad)
    })

    return this
  }

  some (callback) {
    return this.toArray().some(quad => {
      return callback(quad, this)
    })
  }

  toArray () {
    const array = []

    this.forEach(quad => array.push(quad))

    return array
  }

  toStream () {
    const stream = new Readable({
      objectMode: true,
      read: () => {
        this.forEach(quad => {
          stream.push(quad)
        })

        stream.push(null)
      }
    })

    return stream
  }
}

module.exports = AbstractDataset
