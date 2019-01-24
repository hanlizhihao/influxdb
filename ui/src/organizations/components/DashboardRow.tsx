// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'
// Components
import {
    Alignment,
    Button,
    ComponentColor,
    ComponentSize,
    ComponentSpacer,
    ConfirmationButton,
    IconFont,
    IndexList,
    Stack,
} from 'src/clockface'
// Types
import {Dashboard} from 'src/types/v2'
// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface Props {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
}

export default class DashboardRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onDeleteDashboard} = this.props

    return (
      <IndexList.Row key={dashboard.id}>
        <IndexList.Cell>
          <Link to={`/dashboards/${dashboard.id}`}>{dashboard.name}</Link>
        </IndexList.Cell>
        {this.lastModifiedCell}
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ComponentSpacer stackChildren={Stack.Columns} align={Alignment.Left}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text="Clone"
              icon={IconFont.Duplicate}
              titleText="Create a duplicate copy of this Dashboard"
              onClick={this.handleCloneDashboard}
            />
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={onDeleteDashboard}
              returnValue={dashboard}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleCloneDashboard = (): void => {
    const {dashboard, onCloneDashboard} = this.props

    onCloneDashboard(dashboard)
  }

  private get lastModifiedCell(): JSX.Element {
    const {dashboard} = this.props

    const relativeTimestamp = moment(dashboard.meta.updatedAt).fromNow()
    const absoluteTimestamp = moment(dashboard.meta.updatedAt).format(
      UPDATED_AT_TIME_FORMAT
    )

    return (
      <IndexList.Cell>
        <span title={absoluteTimestamp}>{relativeTimestamp}</span>
      </IndexList.Cell>
    )
  }
}
