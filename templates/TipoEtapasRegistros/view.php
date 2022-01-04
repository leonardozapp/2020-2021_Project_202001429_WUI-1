<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoEtapasRegistro $tipoEtapasRegistro
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Tipo Etapas Registro'), ['action' => 'edit', $tipoEtapasRegistro->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Etapas Registro'), ['action' => 'delete', $tipoEtapasRegistro->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoEtapasRegistro->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Etapas Registros'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Etapas Registro'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoEtapasRegistros view content">
            <h3><?= h($tipoEtapasRegistro->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Etapa') ?></th>
                    <td><?= h($tipoEtapasRegistro->etapa) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoEtapasRegistro->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('Fase') ?></th>
                    <td><?= $this->Number->format($tipoEtapasRegistro->fase) ?></td>
                </tr>
                <tr>
                    <th><?= __('PodeLogar') ?></th>
                    <td><?= $tipoEtapasRegistro->podeLogar ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('PodeInvestir') ?></th>
                    <td><?= $tipoEtapasRegistro->podeInvestir ? __('Yes') : __('No'); ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Usuarios') ?></h4>
                <?php if (!empty($tipoEtapasRegistro->usuarios)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Id') ?></th>
                            <th><?= __('Cpf') ?></th>
                            <th><?= __('Nome') ?></th>
                            <th><?= __('Email') ?></th>
                            <th><?= __('Dt Nasc') ?></th>
                            <th><?= __('Senha') ?></th>
                            <th><?= __('Dt Reg') ?></th>
                            <th><?= __('Tipo Plano Id') ?></th>
                            <th><?= __('Tipo Etapas Registro Id') ?></th>
                            <th><?= __('Coment') ?></th>
                            <th><?= __('Created') ?></th>
                            <th><?= __('Modified') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($tipoEtapasRegistro->usuarios as $usuarios) : ?>
                        <tr>
                            <td><?= h($usuarios->id) ?></td>
                            <td><?= h($usuarios->cpf) ?></td>
                            <td><?= h($usuarios->nome) ?></td>
                            <td><?= h($usuarios->email) ?></td>
                            <td><?= h($usuarios->dt_nasc) ?></td>
                            <td><?= h($usuarios->senha) ?></td>
                            <td><?= h($usuarios->dt_reg) ?></td>
                            <td><?= h($usuarios->tipo_plano_id) ?></td>
                            <td><?= h($usuarios->tipo_etapas_registro_id) ?></td>
                            <td><?= h($usuarios->coment) ?></td>
                            <td><?= h($usuarios->created) ?></td>
                            <td><?= h($usuarios->modified) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'Usuarios', 'action' => 'view', $usuarios->id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'Usuarios', 'action' => 'edit', $usuarios->id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'Usuarios', 'action' => 'delete', $usuarios->id], ['confirm' => __('Are you sure you want to delete # {0}?', $usuarios->id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
        </div>
    </div>
</div>
