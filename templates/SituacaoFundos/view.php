<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\SituacaoFundo $situacaoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Situacao Fundo'), ['action' => 'edit', $situacaoFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Situacao Fundo'), ['action' => 'delete', $situacaoFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $situacaoFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Situacao Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Situacao Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="situacaoFundos view content">
            <h3><?= h($situacaoFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $situacaoFundo->has('cnpj_fundo') ? $this->Html->link($situacaoFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $situacaoFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Situacao Fundo') ?></th>
                    <td><?= $situacaoFundo->has('tipo_situacao_fundo') ? $this->Html->link($situacaoFundo->tipo_situacao_fundo->id, ['controller' => 'TipoSituacaoFundos', 'action' => 'view', $situacaoFundo->tipo_situacao_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('DT INI SIT') ?></th>
                    <td><?= h($situacaoFundo->DT_INI_SIT) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT REG CVM') ?></th>
                    <td><?= h($situacaoFundo->DT_REG_CVM) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
