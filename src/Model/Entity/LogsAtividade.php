<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * LogsAtividade Entity
 *
 * @property int $id
 * @property string $action
 * @property string|null $remoteURI
 * @property string|null $localURI
 * @property int|null $result
 * @property bool|null $hasErrors
 * @property string|null $message
 * @property \Cake\I18n\FrozenTime $date
 * @property bool $needToRedo
 */
class LogsAtividade extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'action' => true,
        'remoteURI' => true,
        'localURI' => true,
        'result' => true,
        'hasErrors' => true,
        'message' => true,
        'date' => true,
        'needToRedo' => true,
    ];
}
