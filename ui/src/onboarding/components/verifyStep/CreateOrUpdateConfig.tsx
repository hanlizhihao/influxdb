// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
// Components
import {Spinner} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createDashboardsForPlugins as createDashboardsForPluginsAction} from 'src/protos/actions/'
// Constants
import {TelegrafConfigCreationError, TelegrafConfigCreationSuccess,} from 'src/shared/copy/notifications'
// Types
import {NotificationAction, RemoteDataState} from 'src/types'

export interface OwnProps {
  org: string
  authToken: string
  children: () => JSX.Element
}

export interface DispatchProps {
  notify: NotificationAction
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  createDashboardsForPlugins: typeof createDashboardsForPluginsAction
}

type Props = OwnProps & DispatchProps

interface State {
  loading: RemoteDataState
}

@ErrorHandling
export class CreateOrUpdateConfig extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {
      onSaveTelegrafConfig,
      authToken,
      notify,
      createDashboardsForPlugins,
    } = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      await onSaveTelegrafConfig(authToken)
      notify(TelegrafConfigCreationSuccess)
      await createDashboardsForPlugins()

      this.setState({loading: RemoteDataState.Done})
    } catch (error) {
      notify(TelegrafConfigCreationError)
      this.setState({loading: RemoteDataState.Error})
    }
  }

  public render() {
    return (
      <Spinner loading={this.state.loading}>{this.props.children()}</Spinner>
    )
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSaveTelegrafConfig: createOrUpdateTelegrafConfigAsync,
  createDashboardsForPlugins: createDashboardsForPluginsAction,
}

export default connect<null, DispatchProps, OwnProps>(
  null,
  mdtp
)(CreateOrUpdateConfig)
