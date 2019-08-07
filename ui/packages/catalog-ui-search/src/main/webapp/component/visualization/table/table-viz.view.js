/**
 * Copyright (c) Codice Foundation
 *
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 *
 **/
/*global require*/

import ExportResults from '../../../react-component/container/table-export'
import React from 'react'
import union from 'lodash/union'
const lightboxInstance = require('../../lightbox/lightbox.view.instance.js')
import {
  Button,
  buttonTypeEnum,
} from '../../../react-component/presentation/button'
const Marionette = require('marionette')
const Backbone = require('backbone')
const CustomElements = require('../../../js/CustomElements.js')
const $ = require('jquery')
const TableVisibility = require('./table-visibility.view')
const TableRearrange = require('./table-rearrange.view')
const ResultsTableView = require('../../table/results/table-results.view.js')
const user = require('../../singletons/user-instance.js')
const properties = require('../../../js/properties.js')
const ResultForm = require('../../../component/result-form/result-form.js')

const filterAttributesWithResultForm = (
  attributes,
  resultTemplateAttributes
) => {
  const defaultAttributes = [
    'id',
    'title',
    'metacard-type',
    'metacard-tags',
    'source-id',
  ]
  const filteredAttributes = attributes.filter(attribute =>
    resultTemplateAttributes.includes(attribute)
  )
  return union(filteredAttributes, defaultAttributes).sort()
}
const filteredAttributesModel = Backbone.Model.extend({
  defaults: {
    filteredAttributes: [],
  },
})

module.exports = Marionette.LayoutView.extend({
  tagName: CustomElements.register('table-viz'),
  template() {
    return (
      <React.Fragment>
        <div className="table-empty">
          <h3>Please select a result set to display the table.</h3>
        </div>
        <div className="table-visibility" />
        <div className="table-rearrange" />
        <div className="table-options">
          <button className="options-rearrange is-button">
            Rearrange Columns
            <span className="fa fa-columns" />
          </button>
          <button className="options-visibility is-button">
            Hide/Show Columns
            <span className="fa fa-eye" />
          </button>
          <Button
            buttonType={buttonTypeEnum.neutral}
            text="Export"
            fadeUntilHover
            onClick={this.openExportModal.bind(this)}
          />
        </div>
        <div className="tables-container" />
      </React.Fragment>
    )
  },
  openExportModal() {
    lightboxInstance.model.updateTitle('Export Results')
    lightboxInstance.model.open()
    lightboxInstance.showContent(
      <ExportResults selectionInterface={this.options.selectionInterface} />
    )
  },
  events: {
    'click .options-rearrange': 'startRearrange',
    'click .options-visibility': 'startVisibility',
  },
  regions: {
    table: {
      selector: '.tables-container',
    },
    tableVisibility: {
      selector: '.table-visibility',
      replaceElement: true,
    },
    tableRearrange: {
      selector: '.table-rearrange',
      replaceElement: true,
    },
  },
  initialize: function(options) {
    if (!options.selectionInterface) {
      throw 'Selection interface has not been provided'
    }
    this.filteredAttributes = new filteredAttributesModel()
    this.filterActiveSearchResultsAttributes()

    this.listenTo(
      this.options.selectionInterface,
      'reset:activeSearchResults add:activeSearchResults',
      this.handleEmpty
    )
    this.listenTo(
      this.options.selectionInterface,
      'change:activeSearchResultsAttributes',
      this.filterActiveSearchResultsAttributes
    )
  },
  filterActiveSearchResultsAttributes() {
    const currentQuery = this.options.selectionInterface.getCurrentQuery()
    const resultFormName = currentQuery ? currentQuery.get('detail-level') : ''
    const selectedResultTemplate = ResultForm.getResultCollection().filteredList.filter(
      form => form.id === resultFormName || form.value === resultFormName
    )
    let filteredAttributes = this.options.selectionInterface.getActiveSearchResultsAttributes()

    if (selectedResultTemplate.length > 0) {
      const attrs = this.options.selectionInterface.getActiveSearchResultsAttributes()
      filteredAttributes = filterAttributesWithResultForm(
        attrs,
        selectedResultTemplate[0].descriptors
      )
    }
    this.filteredAttributes.set('filteredAttributes', filteredAttributes)
  },
  handleEmpty: function() {
    this.$el.toggleClass(
      'is-empty',
      this.options.selectionInterface.getActiveSearchResults().length === 0
    )
  },
  onRender: function() {
    this.handleEmpty()
    this.table.show(
      new ResultsTableView({
        selectionInterface: this.options.selectionInterface,
        filteredAttributes: this.filteredAttributes,
      })
    )
  },
  startRearrange: function() {
    this.$el.toggleClass('is-rearranging')
    this.tableRearrange.show(
      new TableRearrange({
        selectionInterface: this.options.selectionInterface,
        filteredAttributes: this.filteredAttributes,
      }),
      {
        replaceElement: true,
      }
    )
  },
  startVisibility: function() {
    this.$el.toggleClass('is-visibilitying')
    this.tableVisibility.show(
      new TableVisibility({
        selectionInterface: this.options.selectionInterface,
        filteredAttributes: this.filteredAttributes,
      }),
      {
        replaceElement: true,
      }
    )
  },
})
