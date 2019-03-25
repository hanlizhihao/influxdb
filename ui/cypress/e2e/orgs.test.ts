import {Organization} from '@influxdata/influx'

const orgRoute = '/organizations'

describe('Orgs', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin()
  })

  describe('Org List', () => {
    beforeEach(() => {
      cy.createOrg().then(({body}) => {
        cy.wrap(body).as('org')
      })

      cy.visit(orgRoute)
    })

    it('can update an org name', () => {
      cy.get<Organization>('@org').then(org => {
        const newName = 'new 🅱️organization'

        cy.visit(`${orgRoute}/${org.id}/members`)

        cy.get('.renamable-page-title--title').click()

        cy.getByTestID('page-header').within(() => {
          cy.getByTestID('input-field')
            .type(newName)
            .type('{enter}')
        })

        cy.visit('/organizations')

        cy.get('.index-list--row').should('contain', newName)
      })
    })

    it('can create an org', () => {
      cy.get('.index-list--row').should('have.length', 2)

      cy.getByTestID('create-org-button').click()

      const orgName = '🅱️organization'

      cy.getByTestID('create-org-name-input').type(orgName)

      cy.getByTestID('create-org-submit-button').click()

      cy.get('.index-list--row')
        .should('contain', orgName)
        .and('have.length', 3)
    })

    it('can delete an org', () => {
      cy.getByTestID('table-row').should('have.length', 2)

      cy.getByTestID('table-row')
        .last()
        .trigger('mouseover')
        .within(() => {
          cy.getByTestID('delete-button')
            .trigger('mouseover')
            .click()

          cy.getByTestID('confirmation-button').click()
        })

      cy.getByTestID('table-row').should('have.length', 1)
    })
  })
})
