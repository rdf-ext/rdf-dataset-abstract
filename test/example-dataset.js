/* global describe */

const ArrayDataset = require('./support/ArrayDataset')

describe('ArrayDataset', () => {
  require('.')(quads => {
    return new ArrayDataset(quads)
  })
})
