// Libraries
import React, {ChangeEvent, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'
// APIs
import {deleteDashboard} from 'src/dashboards/apis'
// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import {ComponentSize, EmptyState, IconFont, Input} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'
import DashboardList from 'src/organizations/components/DashboardList'
// Types
import {Dashboard} from 'src/types/v2'
// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  dashboards: Dashboard[]
  orgName: string
  orgID: string
  onChange: () => void
}

type Props = OwnProps & WithRouterProps

interface State {
  searchTerm: string
}

@ErrorHandling
class Dashboards extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    const {dashboards, orgID, router} = this.props

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            widthPixels={290}
            value={searchTerm}
            onBlur={this.handleFilterBlur}
            onChange={this.handleFilterChange}
            placeholder="Filter Dashboards..."
          />
        </TabbedPageHeader>
        <FilterList<Dashboard>
          searchTerm={searchTerm}
          searchKeys={['name']}
          list={dashboards}
        >
          {ds => (
            <DashboardList
              dashboards={ds}
              emptyState={this.emptyState}
              onDeleteDashboard={this.handleDeleteDashboard}
              orgID={orgID}
              router={router}
            />
          )}
        </FilterList>
      </>
    )
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleDeleteDashboard = async (dashboard: Dashboard) => {
    await deleteDashboard(dashboard)
    this.props.onChange()
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Dashboards , why not create one?`}
            highlightWords={['Dashboards']}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Dashboards match your query" />
      </EmptyState>
    )
  }
}

export default withRouter<OwnProps>(Dashboards)
