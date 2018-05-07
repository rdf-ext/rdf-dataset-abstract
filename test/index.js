/* global describe, it */

const assert = require('assert')
const rdf = require('rdf-data-model')
const Readable = require('stream').Readable

function simpleFilter (subject, predicate, object, graph) {
  return (quad) => {
    return (!subject || quad.subject.equals(subject)) &&
      (!predicate || quad.predicate.equals(predicate)) &&
      (!object || quad.object.equals(object)) &&
      (!graph || quad.graph.equals(graph))
  }
}

function test (datasetFactory) {
  const quadA = rdf.quad(
    rdf.namedNode('http://example.org/subject'),
    rdf.namedNode('http://example.org/predicate'),
    rdf.literal('a'))

  const quadB = rdf.quad(
    rdf.namedNode('http://example.org/subject'),
    rdf.namedNode('http://example.org/predicate'),
    rdf.literal('b'))

  const quadC = rdf.quad(
    rdf.namedNode('http://example.org/subject'),
    rdf.namedNode('http://example.org/predicate'),
    rdf.literal('c'))

  describe('Dataset', () => {
    describe('.length', () => {
      it('should be a number property', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.length, 'number')
      })

      it('should contain the number of quads in the dataset', () => {
        const dataset = datasetFactory([quadA])

        assert.equal(dataset.length, 1)
      })
    })

    describe('.add', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.add, 'function')
      })

      it('should add quads to the dataset', () => {
        const dataset = datasetFactory()

        dataset.add(quadA)

        assert(quadA.equals(dataset.toArray()[0]))
      })

      it('should not create duplicates', () => {
        const dataset = datasetFactory()

        dataset.add(quadA)
        dataset.add(quadA)

        assert.equal(dataset.toArray().length, 1)
      })
    })

    describe('.addAll', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.addAll, 'function')
      })

      it('should import all quads from the given object via the .forEach method', () => {
        const dataset = datasetFactory()

        dataset.addAll([quadA, quadB])

        assert(quadA.equals(dataset.toArray()[0]) || quadA.equals(dataset.toArray()[1]))
        assert(quadB.equals(dataset.toArray()[0]) || quadB.equals(dataset.toArray()[1]))
      })
    })

    describe('.clone', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.clone, 'function')
      })

      it('should return a new dataset instance', () => {
        const dataset = datasetFactory()
        const clone = dataset.clone()

        assert.notEqual(clone, dataset)
        assert.equal(typeof clone.match, 'function')
      })

      it('should create a new dataset instance which contains all quads of the original dataset', () => {
        const dataset = datasetFactory([quadA])
        const clone = dataset.clone()

        assert(quadA.equals(clone.toArray()[0]))
      })
    })

    describe('.difference', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.difference, 'function')
      })

      it('should return a new dataset instance', () => {
        const datasetA = datasetFactory()
        const datasetB = datasetFactory()

        const difference = datasetA.difference(datasetB)

        assert.notEqual(datasetA, difference)
        assert.notEqual(datasetB, difference)
        assert.equal(typeof difference.match, 'function')
      })

      it('should return a dataset which contains the quads which are not included in the other dataset', () => {
        const datasetAB = datasetFactory([quadA, quadB])
        const datasetBC = datasetFactory([quadB, quadC])
        const difference = datasetAB.difference(datasetBC)

        assert.equal(difference.toArray().length, 1)
        assert(quadA.equals(difference.toArray()[0]))
      })
    })

    describe('.every', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.every, 'function')
      })

      it('should return true if every quad pass the filter test', () => {
        const dataset = datasetFactory([quadA, quadB])

        assert.equal(dataset.every(simpleFilter(rdf.namedNode('http://example.org/subject'), null, null)), true)
        assert.equal(dataset.every(simpleFilter(null, null, rdf.literal('a'))), false)
      })
    })

    describe('.filter', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.filter, 'function')
      })

      it('should return a new dataset instance', () => {
        const dataset = datasetFactory()
        const filtered = dataset.filter(() => true)

        assert.notEqual(filtered, dataset)
        assert.equal(typeof filtered.match, 'function')
      })

      it('should return a dataset which contains all quads which pass the filter test', () => {
        const dataset = datasetFactory([quadA, quadB])

        assert.equal(dataset.filter(simpleFilter(rdf.namedNode('http://example.org/subject'), null, null)).length, 2)
        assert.equal(dataset.filter(simpleFilter(null, null, rdf.literal('a'))).length, 1)
        assert.equal(dataset.filter(simpleFilter(null, null, rdf.literal('c'))).length, 0)
      })
    })

    describe('.forEach', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.forEach, 'function')
      })

      it('should call the callback function for every quad', () => {
        const dataset = datasetFactory([quadA, quadB])

        const objects = []

        dataset.forEach((quad) => {
          objects.push(quad.object.value)
        })

        assert.equal(objects.length, 2)
        assert.deepEqual(objects, ['a', 'b'])
      })
    })

    describe('.import', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.import, 'function')
      })

      it('should import quads from stream', () => {
        const stream = new Readable({
          objectMode: true,
          read: () => {}
        })

        stream.push(quadA)
        stream.push(quadB)
        stream.push(null)

        const dataset = datasetFactory()

        return dataset.import(stream).then(() => {
          assert(quadA.equals(dataset.toArray()[0]) || quadA.equals(dataset.toArray()[1]))
          assert(quadB.equals(dataset.toArray()[0]) || quadB.equals(dataset.toArray()[1]))
        })
      })

      it('should forward stream errors', () => {
        const stream = new Readable({
          objectMode: true,
          read: () => {}
        })

        const dataset = datasetFactory()

        const promise = dataset.import(stream)

        stream.emit('error', new Error('example'))

        return new Promise((resolve, reject) => {
          promise.then(() => {
            reject(new Error('no error thrown'))
          }).catch(() => {
            resolve()
          })
        })
      })
    })

    describe('.includes', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.includes, 'function')
      })

      it('should test if the dataset contains the given quad', () => {
        const dataset = datasetFactory([quadA])

        assert(dataset.includes(quadA))
        assert(!dataset.includes(quadB))
      })
    })

    describe('.intersection', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.intersection, 'function')
      })

      it('should return a new dataset instance', () => {
        const datasetA = datasetFactory()
        const datasetB = datasetFactory()
        const intersection = datasetA.intersection(datasetB)

        assert.notEqual(intersection, datasetA)
        assert.notEqual(intersection, datasetB)
        assert.equal(typeof intersection.match, 'function')
      })

      it('should return a dataset with quads included also in the other dataset', () => {
        const datasetA = datasetFactory([quadA, quadB])
        const datasetB = datasetFactory([quadB, quadC])
        const intersection = datasetA.intersection(datasetB)

        assert.equal(intersection.toArray().length, 1)
        assert(quadB.equals(intersection.toArray()[0]))
      })
    })

    describe('.map', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.map, 'function')
      })

      it('should return a new dataset instance', () => {
        const dataset = datasetFactory()
        const mapped = dataset.map(quad => quad)

        assert.notEqual(mapped, dataset)
        assert.equal(typeof mapped.match, 'function')
      })

      it('should call the callback function for every quad', () => {
        const dataset = datasetFactory([quadA])

        const objects = []

        dataset.map(quad => {
          objects.push(quad.object.value)

          return quad
        })

        assert.deepEqual(objects, ['a'])
      })

      it('should call the callback function for every quad and return a dataset which contains the quads returend by the callback', () => {
        const dataset = datasetFactory([quadA])

        const mapped = dataset.map(() => {
          return quadB
        })

        assert.equal(mapped.length, 1)
        assert(quadB.equals(mapped.toArray()[0]))
      })
    })

    describe('.match', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.match, 'function')
      })

      it('should return a new dataset instance', () => {
        const dataset = datasetFactory()
        const matches = dataset.match()

        assert.notEqual(matches, dataset)
        assert.equal(typeof matches.match, 'function')
      })

      it('should return a dataset which contains all quads which pass the subject match pattern', () => {
        const subject1 = rdf.namedNode('http://example.org/subject1')
        const subject2 = rdf.namedNode('http://example.org/subject2')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object = rdf.namedNode('http://example.org/object')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject1, predicate, object, graph)
        const quad2 = rdf.quad(subject2, predicate, object, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.match(rdf.namedNode('http://example.org/subject1'), null, null, null).toArray().length, 1)
        assert.equal(dataset.match(rdf.namedNode('http://example.org/subject2'), null, null, null).toArray().length, 1)
        assert.equal(dataset.match(rdf.namedNode('http://example.org/subject3'), null, null, null).toArray().length, 0)
      })

      it('should return a dataset which contains all quads which pass the predicate match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate1 = rdf.namedNode('http://example.org/predicate1')
        const predicate2 = rdf.namedNode('http://example.org/predicate2')
        const object = rdf.namedNode('http://example.org/object')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject, predicate1, object, graph)
        const quad2 = rdf.quad(subject, predicate2, object, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.match(null, rdf.namedNode('http://example.org/predicate1'), null, null).toArray().length, 1)
        assert.equal(dataset.match(null, rdf.namedNode('http://example.org/predicate2'), null, null).toArray().length, 1)
        assert.equal(dataset.match(null, rdf.namedNode('http://example.org/predicate3'), null, null).toArray().length, 0)
      })

      it('should return a dataset which contains all quads which pass the object match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object1 = rdf.namedNode('http://example.org/object1')
        const object2 = rdf.namedNode('http://example.org/object2')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject, predicate, object1, graph)
        const quad2 = rdf.quad(subject, predicate, object2, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.match(null, null, rdf.namedNode('http://example.org/object1'), null).toArray().length, 1)
        assert.equal(dataset.match(null, null, rdf.namedNode('http://example.org/object2'), null).toArray().length, 1)
        assert.equal(dataset.match(null, null, rdf.namedNode('http://example.org/object3'), null).toArray().length, 0)
      })

      it('should return a dataset which contains all quads which pass the graph match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object = rdf.namedNode('http://example.org/object')
        const graph1 = rdf.namedNode('http://example.org/graph1')
        const graph2 = rdf.namedNode('http://example.org/graph2')
        const quad1 = rdf.quad(subject, predicate, object, graph1)
        const quad2 = rdf.quad(subject, predicate, object, graph2)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.match(null, null, null, rdf.namedNode('http://example.org/graph1')).toArray().length, 1)
        assert.equal(dataset.match(null, null, null, rdf.namedNode('http://example.org/graph2')).toArray().length, 1)
        assert.equal(dataset.match(null, null, null, rdf.namedNode('http://example.org/graph3')).toArray().length, 0)
      })
    })

    describe('.merge', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.merge, 'function')
      })

      it('should return a new dataset instance', () => {
        const datasetA = datasetFactory()
        const datasetB = datasetFactory()
        const merged = datasetA.merge(datasetB)

        assert.notEqual(merged, datasetA)
        assert.notEqual(merged, datasetB)
        assert.equal(typeof merged.match, 'function')
      })

      it('should not change the dataset object and the given dataset', () => {
        const datasetA = datasetFactory([quadA])
        const datasetB = datasetFactory([quadB])

        datasetA.merge(datasetB)

        assert.equal(datasetA.toArray().length, 1)
        assert(quadA.equals(datasetA.toArray()[0]))
        assert.equal(datasetB.toArray().length, 1)
        assert(quadB.equals(datasetB.toArray()[0]))
      })

      it('should return a dataset which contains all quads from the dataset object and the given dataset', () => {
        const datasetA = datasetFactory([quadA])
        const datasetB = datasetFactory([quadB])
        const merged = datasetA.merge(datasetB)

        assert.equal(merged.toArray().length, 2)
        assert(quadA.equals(merged.toArray()[0]) || quadA.equals(merged.toArray()[1]))
        assert(quadB.equals(merged.toArray()[0]) || quadB.equals(merged.toArray()[1]))
      })
    })

    describe('.remove', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.remove, 'function')
      })

      it('should remove the given quad', () => {
        const dataset = datasetFactory([quadA, quadB])

        dataset.remove(quadB)

        assert.equal(dataset.toArray().length, 1)
        assert(quadA.equals(dataset.toArray()[0]))
      })
    })

    describe('.removeMatches', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.removeMatches, 'function')
      })

      it('should return the dataset object itself', () => {
        const dataset = datasetFactory()

        const result = dataset.removeMatches()

        assert.equal(result, dataset)
      })

      it('should remove all quads which pass the subject match pattern', () => {
        const subject1 = rdf.namedNode('http://example.org/subject1')
        const subject2 = rdf.namedNode('http://example.org/subject2')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object = rdf.namedNode('http://example.org/object')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject1, predicate, object, graph)
        const quad2 = rdf.quad(subject2, predicate, object, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.removeMatches(rdf.namedNode('http://example.org/subject3'), null, null, null).toArray().length, 2)
        assert.equal(dataset.removeMatches(rdf.namedNode('http://example.org/subject2'), null, null, null).toArray().length, 1)
        assert.equal(dataset.removeMatches(rdf.namedNode('http://example.org/subject1'), null, null, null).toArray().length, 0)
      })

      it('should remove all quads which pass the predicate match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate1 = rdf.namedNode('http://example.org/predicate1')
        const predicate2 = rdf.namedNode('http://example.org/predicate2')
        const object = rdf.namedNode('http://example.org/object')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject, predicate1, object, graph)
        const quad2 = rdf.quad(subject, predicate2, object, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.removeMatches(null, rdf.namedNode('http://example.org/predicate3'), null, null).toArray().length, 2)
        assert.equal(dataset.removeMatches(null, rdf.namedNode('http://example.org/predicate2'), null, null).toArray().length, 1)
        assert.equal(dataset.removeMatches(null, rdf.namedNode('http://example.org/predicate1'), null, null).toArray().length, 0)
      })

      it('should remove all quads that pass the object match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object1 = rdf.namedNode('http://example.org/object1')
        const object2 = rdf.namedNode('http://example.org/object2')
        const graph = rdf.namedNode('http://example.org/graph')
        const quad1 = rdf.quad(subject, predicate, object1, graph)
        const quad2 = rdf.quad(subject, predicate, object2, graph)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.removeMatches(null, null, rdf.namedNode('http://example.org/object3'), null).toArray().length, 2)
        assert.equal(dataset.removeMatches(null, null, rdf.namedNode('http://example.org/object2'), null).toArray().length, 1)
        assert.equal(dataset.removeMatches(null, null, rdf.namedNode('http://example.org/object1'), null).toArray().length, 0)
      })

      it('should remove all quads which pass the graph match pattern', () => {
        const subject = rdf.namedNode('http://example.org/subject')
        const predicate = rdf.namedNode('http://example.org/predicate')
        const object = rdf.namedNode('http://example.org/object')
        const graph1 = rdf.namedNode('http://example.org/graph1')
        const graph2 = rdf.namedNode('http://example.org/graph2')
        const quad1 = rdf.quad(subject, predicate, object, graph1)
        const quad2 = rdf.quad(subject, predicate, object, graph2)
        const dataset = datasetFactory([quad1, quad2])

        assert.equal(dataset.removeMatches(null, null, null, rdf.namedNode('http://example.org/graph3')).toArray().length, 2)
        assert.equal(dataset.removeMatches(null, null, null, rdf.namedNode('http://example.org/graph2')).toArray().length, 1)
        assert.equal(dataset.removeMatches(null, null, null, rdf.namedNode('http://example.org/graph1')).toArray().length, 0)
      })
    })

    describe('.some', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.some, 'function')
      })

      it('should return true if any quad passes the filter test', () => {
        const dataset = datasetFactory([quadA, quadB])

        assert(dataset.some(simpleFilter(rdf.namedNode('http://example.org/subject'), null, null)))
      })

      it('should return false if no quad passes the filter test', () => {
        const dataset = datasetFactory([quadA, quadB])

        assert(!dataset.some(simpleFilter(rdf.namedNode('http://example.org/subject1'), null, null)))
      })
    })

    describe('.toArray', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.toArray, 'function')
      })

      it('should return all quads in an array', () => {
        const dataset = datasetFactory([quadA, quadB])
        const array = dataset.toArray()

        assert(quadA.equals(array[0]) || quadA.equals(array[1]))
        assert(quadB.equals(array[0]) || quadB.equals(array[1]))
      })
    })

    describe('.toStream', () => {
      it('should be a method', () => {
        const dataset = datasetFactory()

        assert.equal(typeof dataset.toStream, 'function')
      })

      it('should return a stream', () => {
        const dataset = datasetFactory()
        const stream = dataset.toStream()

        assert(stream.readable)
      })

      it('should return a stream which emits all quads of the dataset', () => {
        const dataset = datasetFactory([quadA])
        const stream = dataset.toStream()
        const output = []

        return new Promise((resolve, reject) => {
          stream.on('end', () => {
            assert.equal(output.length, 1)
            assert(quadA.equals(output[0]))

            resolve()
          })

          stream.on('error', reject)

          stream.on('data', (quad) => {
            output.push(quad)
          })
        })
      })
    })
  })
}

module.exports = test
